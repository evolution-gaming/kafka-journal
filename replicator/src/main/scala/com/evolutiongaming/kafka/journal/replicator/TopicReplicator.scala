package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.implicits._
import cats.{Applicative, FlatMap, Monad, Parallel, ~>}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.conversions.{ConsumerRecordToActionRecord, PayloadToEvents}
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.util.Named
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Bytes => _, _}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.{CollectorRegistry, LabelNames, Quantile, Quantiles}
import scodec.bits.ByteVector

import scala.concurrent.duration._


// TODO partition replicator ?
// TODO add metric to track replication lag in case it cannot catchup with producers
// TODO verify that first consumed offset matches to the one expected, otherwise we screwed.
// TODO should it be Resource ?
trait TopicReplicator[F[_]] {

  def done: F[Unit]

  def close: F[Unit]
}

object TopicReplicator { self =>

  // TODO should return Resource
  def of[F[_] : Concurrent : Timer : Parallel : LogOf : FromTry](
    topic: Topic,
    journal: ReplicatedJournal[F],
    consumer: Resource[F, Consumer[F]],
    metrics: Metrics[F]
  ): F[TopicReplicator[F]] = {

    implicit val fromAttempt = FromAttempt.lift[F]
    implicit val fromJsResult = FromJsResult.lift[F]

    def apply(stopRef: StopRef[F], fiber: Fiber[F, Unit])(implicit log: Log[F]) = {
      self.apply[F](stopRef, fiber)
    }

    def start(
      stopRef: StopRef[F],
      consumer: Resource[F, Consumer[F]],
      random: Random.State)(implicit
      log: Log[F]
    ) = {
      
      val strategy = Strategy
        .fullJitter(100.millis, random)
        .limit(1.minute)
        .resetAfter(5.minutes)

      val onError = OnError.fromLog(log)

      val retry = Retry(strategy, onError)

      val payloadToEvents = PayloadToEvents[F]
      val consumerRecordToActionRecord = ConsumerRecordToActionRecord[F]
      retry {
        consumer.use { consumer =>
          of(
            topic,
            stopRef,
            consumer,
            1.hour,
            consumerRecordToActionRecord,
            payloadToEvents,
            journal,
            metrics)
        }
      }.start
    }

    for {
      log0    <- LogOf[F].apply(TopicReplicator.getClass)
      log      = log0 prefixed topic
      stopRef <- StopRef.of[F]
      random  <- Random.State.fromClock[F]()
      fiber   <- start(stopRef, consumer, random)(log)
    } yield {
      apply(stopRef, fiber)(log)
    }
  }

  //  TODO return error in case failed to connect
  def apply[F[_] : FlatMap : Log](stopRef: StopRef[F], fiber: Fiber[F, Unit]): TopicReplicator[F] = {
    new TopicReplicator[F] {

      def done = fiber.join

      def close = {
        for {
          _ <- Log[F].debug("shutting down")
          _ <- stopRef.set // TODO remove
          _ <- fiber.join
          //            _ <- fiber.cancel // TODO
        } yield {}
      }
    }
  }

  //  TODO return error in case failed to connect
  def of[F[_] : Concurrent : Clock : Parallel : Log : FromTry](
    topic: Topic,
    stopRef: StopRef[F],
    consumer: Consumer[F],
    errorCooldown: FiniteDuration,
    consumerRecordToActionRecord: ConsumerRecordToActionRecord[F],
    payloadToEvents: PayloadToEvents[F],
    journal: ReplicatedJournal[F],
    metrics: Metrics[F]
  ): F[Unit] = {

    final case class Partition(records: Nel[ConsumerRecord[String, ByteVector]], offset: Offset)

    def round(
      state: State,
      consumerRecords: Map[TopicPartition, List[ConsumerRecord[String, ByteVector]]],
      roundStart: Instant
    ): F[State] = {

      val records = for {
        records <- consumerRecords.values.toList
        record  <- records
      } yield record

      val ios = for {
        records <- records.traverseFilter { record => consumerRecordToActionRecord(record) }
      } yield for {
        (key, records) <- records.groupBy(_.action.key)
      } yield {

        val head = records.head
        val id = key.id

        def measurements(records: Int) = {
          for {
            now <- Clock[F].instant
          } yield {
            Metrics.Measurements(
              partition = head.partition,
              replicationLatency = now diff head.action.timestamp,
              deliveryLatency = roundStart diff head.action.timestamp,
              records = records)
          }
        }

        def delete(partitionOffset: PartitionOffset, deleteTo: SeqNr, origin: Option[Origin]) = {
          for {
            _            <- journal.delete(key, partitionOffset, roundStart, deleteTo, origin)
            measurements <- measurements(1)
            latency       = measurements.replicationLatency
            _            <- metrics.delete(measurements)
            _            <- Log[F].info {
              val originStr = origin.fold("") { origin => s", origin: $origin" }
              s"delete in ${ latency.toMillis }ms, id: $id, offset: $partitionOffset, deleteTo: $deleteTo$originStr"
            }
          } yield {}
        }

        def append(partitionOffset: PartitionOffset, records: Nel[ActionRecord[Action.Append]]) = {

          val bytes = records.foldLeft(0L) { case (bytes, record) => bytes + record.action.payload.size }

          val events = records.flatTraverse { record =>
            val action = record.action
            val payloadAndType = PayloadAndType(action)
            for {
              events <- payloadToEvents(payloadAndType)
            } yield for {
              event <- events.events
            } yield {
              EventRecord(record, event)
            }
          }

          val expireAfter = records.last.action.header.expireAfter

          for {
            events       <- events
            _            <- journal.append(key, partitionOffset, roundStart, expireAfter = expireAfter, events)
            measurements <- measurements(records.size)
            _            <- metrics.append(
              events = events.length,
              bytes = bytes,
              measurements = measurements)
            latency       = measurements.replicationLatency
            _            <- Log[F].info {
              val seqNrs =
                if (events.tail.isEmpty) s"seqNr: ${ events.head.seqNr }"
                else s"seqNrs: ${ events.head.seqNr }..${ events.last.seqNr }"
              val origin = records.head.action.origin
              val originStr = origin.fold("") { origin => s", origin: $origin" }
              s"append in ${ latency.toMillis }ms, id: $id, offset: $partitionOffset, $seqNrs$originStr"
            }
          } yield {}
        }

        Batch.list(records).foldLeft(().pure[F]) { (result, batch) =>
          for {
            _ <- result
            _ <- batch match {
              case batch: Batch.Appends => append(batch.partitionOffset, batch.records)
              case batch: Batch.Delete  => delete(batch.partitionOffset, batch.seqNr, batch.origin)
            }
          } yield {}
        }
      }

      val offsets = for {
        (topicPartition, records) <- consumerRecords
      } yield {
        val offset = records.foldLeft(Offset.Min) { (offset, record) => record.offset max offset }
        (topicPartition.partition, offset)
      }

      for {
        ios      <- ios
        _        <- ios.toList.parFold
        pointers  = TopicPointers(offsets)
        _        <- if (offsets.isEmpty) ().pure[F] else journal.save(topic, pointers, roundStart)
      } yield {
        state.copy(pointers = state.pointers + pointers)
      }
    }

    def ifContinue(fa: F[Either[State, Unit]]) = {
      for {
        stop   <- stopRef.get
        result <- if (stop) ().asRight[State].pure[F] else fa
      } yield result
    }

    def commit(records: Map[TopicPartition, Nel[ConsumerRecord[String, ByteVector]]], state: State) = {

      val offsets = for {
        (topicPartition, records) <- records
      } yield {
        val offset = records.foldLeft(Offset.Min) { (offset, record) => record.offset max offset }
        val offsetAndMetadata = OffsetAndMetadata(offset + 1/*TODO pass metadata/origin*/)
        (topicPartition, offsetAndMetadata)
      }

      consumer.commit(offsets)
        .as(state)
        .handleErrorWith { error =>
        for {
          now   <- Clock[F].instant
          state <- state.failed.fold {
            state.copy(failed = now.some).pure[F]
          } { failed =>
            if (failed + errorCooldown <= now) state.copy(failed = now.some).pure[F]
            else error.raiseError[F, State]
          }
        } yield state
      }
    }

    def consume(state: State): F[Either[State, Unit]] = {

      def consume(
        roundStart: Instant,
        consumerRecords: Map[TopicPartition, Nel[ConsumerRecord[String, ByteVector]]]
      ) = {
        val records = for {
          (topicPartition, records) <- consumerRecords
          partition                  = topicPartition.partition
          offset                     = state.pointers.values.get(partition)
          records1                   = records.toList
          result                     = offset.fold(records1) { offset => records1.filter(_.offset > offset) }
          if result.nonEmpty
        } yield {
          (topicPartition, result)
        }

        for {
          state    <- if (records.isEmpty) state.pure[F] else round(state, records.toMap, roundStart)
          state    <- commit(consumerRecords, state)
          roundEnd <- Clock[F].instant
          records   = consumerRecords.foldLeft(0) { case (acc, (_, record)) => acc + record.size }
          _        <- metrics.round(roundEnd diff roundStart, records)
        } yield state.asLeft[Unit]
      }


      ifContinue {
        for {
          timestamp       <- Clock[F].instant
          consumerRecords <- consumer.poll
          records          = consumerRecords.values
          state           <- {
            if (records.isEmpty) state.asLeft[Unit].pure[F]
            else ifContinue { consume(timestamp, records) }
          }
        } yield state
      }
    }

    for {
      pointers <- journal.pointers(topic)
      _        <- consumer.subscribe(topic)
      _        <- State(pointers, none).tailRecM(consume)
    } yield {}
  }


  final case class State(pointers: TopicPointers, failed: Option[Instant])


  trait Consumer[F[_]] {

    def subscribe(topic: Topic): F[Unit]

    def poll: F[ConsumerRecords[String, ByteVector]]

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

    def assignment: F[Set[TopicPartition]]
  }

  object Consumer {

    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    def apply[F[_] : Applicative](
      consumer: KafkaConsumer[F, String, ByteVector],
      pollTimeout: FiniteDuration
    ): Consumer[F] = {
      new Consumer[F] {

        def subscribe(topic: Topic) = consumer.subscribe(topic)

        def poll = consumer.poll(pollTimeout)

        def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = consumer.commit(offsets)

        def assignment = consumer.assignment
      }
    }

    def of[F[_] : Sync : KafkaConsumerOf : FromTry](
      topic: Topic,
      config: ConsumerConfig,
      pollTimeout: FiniteDuration,
      hostName: Option[HostName]
    ): Resource[F, Consumer[F]] = {

      val groupId = {
        val prefix = config.groupId getOrElse "replicator"
        s"$prefix-$topic"
      }

      val common = config.common

      val clientId = {
        val clientId = common.clientId getOrElse "replicator"
        hostName.fold(clientId) { hostName => s"$clientId-$hostName" }
      }

      val config1 = config.copy(
        common = common.copy(clientId = clientId.some),
        groupId = Some(groupId),
        autoOffsetReset = AutoOffsetReset.Earliest,
        autoCommit = false)

      for {
        consumer <- KafkaConsumerOf[F].apply[String, ByteVector](config1)
      } yield {
        Consumer[F](consumer, pollTimeout)
      }
    }


    implicit class ConsumerOps[F[_]](val self: Consumer[F]) extends AnyVal {

      def mapK[G[_]](f: F ~> G): Consumer[G] = new Consumer[G] {

        def subscribe(topic: Topic) = f(self.subscribe(topic))

        def poll = f(self.poll)

        def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = f(self.commit(offsets))

        def assignment = f(self.assignment)
      }


      def mapMethod(f: Named[F]): Consumer[F] = new Consumer[F] {

        def subscribe(topic: Topic) = f(self.subscribe(topic), "subscribe")

        def poll = f(self.poll, "poll")

        def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = f(self.commit(offsets), "commit")

        def assignment = f(self.assignment, "assignment")
      }
    }
  }


  trait StopRef[F[_]] {

    def set: F[Unit]

    def get: F[Boolean]
  }

  object StopRef {

    def apply[F[_]](implicit F: StopRef[F]): StopRef[F] = F

    def of[F[_] : Sync]: F[StopRef[F]] = {
      for {
        ref <- Ref.of[F, Boolean](false)
      } yield {
        new StopRef[F] {
          def set = ref.set(true)
          def get = ref.get
        }
      }
    }
  }


  trait Metrics[F[_]] {
    import Metrics._

    def append(events: Int, bytes: Long, measurements: Measurements): F[Unit]

    def delete(measurements: Measurements): F[Unit]

    def round(latency: FiniteDuration, records: Int): F[Unit]
  }

  object Metrics {

    def apply[F[_]](implicit F: Metrics[F]): Metrics[F] = F

    def empty[F[_] : Applicative]: Metrics[F] = const(Applicative[F].unit)

    def const[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def append(events: Int, bytes: Long, measurements: Measurements) = unit

      def delete(measurements: Measurements) = unit

      def round(duration: FiniteDuration, records: Int) = unit
    }


    def of[F[_] : Monad](
      registry: CollectorRegistry[F],
      prefix: String = "replicator"
    ): Resource[F, Topic => TopicReplicator.Metrics[F]] = {

      val quantiles = Quantiles(
        Quantile(0.9, 0.05),
        Quantile(0.99, 0.005))

      val replicationSummary = registry.summary(
        name      = s"${ prefix }_replication_latency",
        help      = "Replication latency in seconds",
        quantiles = quantiles,
        labels    = LabelNames("topic", "partition", "type"))

      val deliverySummary = registry.summary(
        name      = s"${ prefix }_delivery_latency",
        help      = "Delivery latency in seconds",
        quantiles = quantiles,
        labels    = LabelNames("topic", "partition", "type"))

      val eventsSummary = registry.summary(
        name      = s"${ prefix }_events",
        help      = "Number of events replicated",
        quantiles = Quantiles.Empty,
        labels    = LabelNames("topic", "partition"))

      val bytesSummary = registry.summary(
        name      = s"${ prefix }_bytes",
        help      = "Number of bytes replicated",
        quantiles = Quantiles.Empty,
        labels    = LabelNames("topic", "partition"))

      val recordsSummary = registry.summary(
        name      = s"${ prefix }_records",
        help      = "Number of kafka records processed",
        quantiles = Quantiles.Empty,
        labels    = LabelNames("topic", "partition"))

      val roundSummary = registry.summary(
        name      = s"${ prefix }_round_duration",
        help      = "Replication round duration",
        quantiles = quantiles,
        labels    = LabelNames("topic"))

      val roundRecordsSummary = registry.summary(
        name      = s"${ prefix }_round_records",
        help      = "Number of kafka records processed in round",
        quantiles = Quantiles.Empty,
        labels    = LabelNames("topic"))

      for {
        replicationSummary  <- replicationSummary
        deliverySummary     <- deliverySummary
        eventsSummary       <- eventsSummary
        bytesSummary        <- bytesSummary
        recordsSummary      <- recordsSummary
        roundSummary        <- roundSummary
        roundRecordsSummary <- roundRecordsSummary
      } yield {

        topic: Topic => {

          def observeMeasurements(name: String, measurements: Measurements) = {

            val partition = measurements.partition.toString
            for {
              _ <- replicationSummary.labels(topic, partition, name).observe(measurements.replicationLatency.toNanos.nanosToSeconds)
              _ <- deliverySummary.labels(topic, partition, name).observe(measurements.deliveryLatency.toNanos.nanosToSeconds)
              _ <- recordsSummary.labels(topic, partition).observe(measurements.records.toDouble)
            } yield {}
          }

          new TopicReplicator.Metrics[F] {

            def append(events: Int, bytes: Long, measurements: Measurements) = {
              val partition = measurements.partition.toString
              for {
                _ <- observeMeasurements("append", measurements)
                _ <- eventsSummary.labels(topic, partition).observe(events.toDouble)
                _ <- bytesSummary.labels(topic, partition).observe(bytes.toDouble)
              } yield {}
            }

            def delete(measurements: Measurements) = {
              observeMeasurements("delete", measurements)
            }

            def round(duration: FiniteDuration, records: Int) = {
              for {
                _ <- roundSummary.labels(topic).observe(duration.toNanos.nanosToSeconds)
                _ <- roundRecordsSummary.labels(topic).observe(records.toDouble)
              } yield {}
            }
          }
        }
      }
    }


    final case class Measurements(
      partition: Partition,
      replicationLatency: FiniteDuration,
      deliveryLatency: FiniteDuration,
      records: Int)
  }
}


