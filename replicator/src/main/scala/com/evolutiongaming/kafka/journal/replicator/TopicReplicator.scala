package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect._
import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.implicits._
import cats.{Applicative, Monad, Parallel, ~>}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.conversions.{ConsumerRecordToActionRecord, PayloadToEvents}
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.util.CollectionHelper._
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.kafka.journal.util.{Named, ResourceOf}
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Bytes => _, _}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.{CollectorRegistry, LabelNames, Quantile, Quantiles}
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import scala.concurrent.duration._


// TODO add metric to track replication lag in case it cannot catchup with producers
// TODO verify that first consumed offset matches to the one expected, otherwise we screwed.
// TODO should it be Resource ?
object TopicReplicator {

  def of[F[_] : Concurrent : Timer : Parallel : LogOf : FromTry](
    topic: Topic,
    journal: ReplicatedJournal[F],
    consumer: Resource[F, Consumer[F]],
    metrics: Metrics[F]
  ): Resource[F, F[Unit]] = {

    implicit val fromAttempt = FromAttempt.lift[F]
    implicit val fromJsResult = FromJsResult.lift[F]
    val payloadToEvents = PayloadToEvents[F]

    def consume(
      consumer: Resource[F, Consumer[F]],
      log: Log[F]
    ) = {

      val consumerRecordToActionRecord = ConsumerRecordToActionRecord[F]
      of(
        topic = topic,
        consumer = consumer,
        consumerRecordToActionRecord = consumerRecordToActionRecord,
        payloadToEvents = payloadToEvents,
        journal = journal,
        metrics = metrics,
        log = log)
    }

    def log = for {
      log <- LogOf[F].apply(TopicReplicator.getClass)
    } yield {
      log prefixed topic
    }

    for {
      log       <- Resource.liftF(log)
      consuming  = consume(consumer, log).start
      consuming <- ResourceOf(consuming)
    } yield consuming
  }

  def of[F[_] : Concurrent : Parallel : FromTry : Timer](
    topic: Topic,
    consumer: Resource[F, Consumer[F]],
    consumerRecordToActionRecord: ConsumerRecordToActionRecord[F],
    payloadToEvents: PayloadToEvents[F],
    journal: ReplicatedJournal[F],
    metrics: Metrics[F],
    log: Log[F],
  ): F[Unit] = {

    val replicateRecords = ReplicateRecords(
      topic = topic,
      consumerRecordToActionRecord = consumerRecordToActionRecord,
      journal = journal,
      metrics = metrics,
      payloadToEvents = payloadToEvents,
      log = log)

    of(
      topic = topic,
      log = log,
      consumer = consumer,
      journal = journal,
      metrics = metrics,
      replicateRecords = replicateRecords)
  }

  def of[F[_] : Concurrent : Clock : Parallel : FromTry : Timer](
    topic: Topic,
    log: Log[F],
    consumer: Resource[F, Consumer[F]],
    journal: ReplicatedJournal[F],
    metrics: Metrics[F],
    replicateRecords: ReplicateRecords[F],
  ): F[Unit] = {

    def consume(state: F[State], records: Nem[TopicPartition, Nel[ConsRecord]]): F[State] = {

      def consume(
        state: State,
        roundStart: Instant,
        consumerRecords: Nem[TopicPartition, Nel[ConsRecord]]
      ) = {

        val records = for {
          (partition, records) <- consumerRecords.toSortedMap
          offset                = state.pointers.values.get(partition.partition)
          records              <- offset.fold(records.some) { offset => records.filter { _.offset > offset }.toNel }
        } yield {
          (partition, records)
        }

        for {
          state    <- records.toNem.fold(state.pure[F]) { records => replicateRecords(state, records, roundStart) }
          records   = consumerRecords.foldLeft(0) { case (size, records) => size + records.size }
          roundEnd <- Clock[F].instant
          _        <- metrics.round(roundEnd diff roundStart, records)
        } yield state
      }

      for {
        timestamp <- Clock[F].instant
        state     <- state
        state     <- consume(state, timestamp, records)
      } yield {
        state
      }
    }

    val consumer1 = consumer.map { consumer =>

      new ConsumeTopic.Consumer[F] {

        def subscribe(listener: RebalanceListener[F]) = {
          consumer.subscribe(topic) // TODO pass listener
        }

        def poll = consumer.poll

        def commit(offsets: Nem[Partition, Offset]) = {
          val offsets1 = offsets.mapKV { case (partition, offset) =>
            val partition1 = TopicPartition(topic = topic, partition = partition)
            (partition1, offset)
          }
          consumer.commit(offsets1).handleErrorWith { a => log.error(s"commit failed for $offsets: $a") }
        }
      }
    }

    val topicFlowOf: TopicFlowOf[F] = {
      (topic: Topic) => {
        val topicFlowOf = for {
          pointers <- journal.pointers(topic)
          stateRef <- Ref.of(State(pointers, none))
        } yield {
          new TopicFlow[F] {

            def assign(partitions: Nel[Partition]) = ().pure[F]

            def apply(records: Nem[TopicPartition, Nel[ConsRecord]]) = {
              for {
                state <- consume(stateRef.get, records)
                _     <- stateRef.set(state)
              } yield {
                records.toSortedMap.map { case (partition, records) =>
                  val offset = records.foldLeft {
                    Offset.Min
                  } { (offset, record) =>
                    record.offset + 1 max offset
                  }
                  (partition.partition, offset)
                }
              }
            }

            def revoke(partitions: Nel[Partition]) = ().pure[F]
          }
        }

        Resource.liftF(topicFlowOf)
      }
    }

    ConsumeTopic(topic, consumer1, topicFlowOf, log)
  }


  final case class State(pointers: TopicPointers, failed: Option[Instant])


  trait Consumer[F[_]] {

    def subscribe(topic: Topic): F[Unit]

    def poll: Stream[F, ConsRecords]

    def commit(offsets: Nem[TopicPartition, Offset]): F[Unit]
  }

  object Consumer {

    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    def apply[F[_] : Monad](
      pollTimeout: FiniteDuration,
      hostName: Option[HostName],
      consumer: KafkaConsumer[F, String, ByteVector],
      commit: Commit[F]
    ): Consumer[F] = {
      val metadata = hostName.fold { Metadata.empty } { _.value }
      val commit1 = commit
      new Consumer[F] {

        def subscribe(topic: Topic) = consumer.subscribe(topic, none)

        def poll = Stream.repeat(consumer.poll(pollTimeout))

        def commit(offsets: Nem[TopicPartition, Offset]) = {
          val offsets1 = offsets.map { offset => OffsetAndMetadata(offset, metadata) }
          commit1(offsets1)
        }
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
        groupId = groupId.some,
        autoOffsetReset = AutoOffsetReset.Earliest,
        autoCommit = false)

      for {
        consumer <- KafkaConsumerOf[F].apply[String, ByteVector](config1)
      } yield {
        val commit = Commit(consumer)
        Consumer[F](pollTimeout, hostName, consumer, commit)
      }
    }


    implicit class ConsumerOps[F[_]](val self: Consumer[F]) extends AnyVal {

      def mapK[G[_]](fg: F ~> G, gf: G ~> F): Consumer[G] = new Consumer[G] {

        def subscribe(topic: Topic) = fg(self.subscribe(topic))

        def poll = self.poll.mapK(fg, gf)

        def commit(offsets: Nem[TopicPartition, Offset]) = fg(self.commit(offsets))
      }


      def mapMethod(f: Named[F]): Consumer[F] = new Consumer[F] {

        def subscribe(topic: Topic) = f(self.subscribe(topic), "subscribe")

        def poll = self.poll

        def commit(offsets: Nem[TopicPartition, Offset]) = f(self.commit(offsets), "commit")
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


