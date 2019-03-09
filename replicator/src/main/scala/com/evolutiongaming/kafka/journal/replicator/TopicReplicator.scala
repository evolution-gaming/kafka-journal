package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.temp.par._
import cats.{Applicative, FlatMap, ~>}
import com.evolutiongaming.kafka.journal.CatsHelper._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.random.Random
import com.evolutiongaming.kafka.journal.util.Named
import com.evolutiongaming.kafka.journal.util.TimeHelper._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Bytes => _, _}

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
  def of[F[_] : Concurrent : Timer : Par : Metrics : ReplicatedJournal : ContextShift : LogOf](
    topic: Topic,
    consumer: Resource[F, Consumer[F]]
  ): F[TopicReplicator[F]] = {

    def apply(stopRef: StopRef[F], fiber: Fiber[F, Unit])(implicit log: Log[F]) = {
      self.apply[F](stopRef, fiber)
    }

    def start(stopRef: StopRef[F], consumer: Resource[F, Consumer[F]], random: Random.State)(implicit log: Log[F]) = {
      val strategy = Retry.Strategy
        .fullJitter(100.millis, random)
        .limit(1.minute)
        .resetAfter(5.minutes)

      val retry = RetryOf[F](strategy)
      Concurrent[F].start {
        retry {
          consumer.use { consumer => of(topic, stopRef, consumer, 1.hour) }
        }
      }
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
  def of[F[_] : Concurrent : Clock : Par : Metrics : ReplicatedJournal : Log : ContextShift](
    topic: Topic,
    stopRef: StopRef[F],
    consumer: Consumer[F],
    errorCooldown: FiniteDuration
  ): F[Unit] = {

    def round(
      state: State,
      consumerRecords: Map[TopicPartition, List[ConsumerRecord[Id, Bytes]]],
      roundStart: Instant): F[(State, Map[TopicPartition, OffsetAndMetadata])] = {

      val records = for {
        records <- consumerRecords.values.toList
        record  <- records
        action  <- record.toActionRecord
      } yield action

      val ios = for {
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
            _            <- ReplicatedJournal[F].delete(key, partitionOffset, roundStart, deleteTo, origin)
            measurements <- measurements(1)
            latency       = measurements.replicationLatency
            _            <- Metrics[F].delete(measurements)
            _            <- Log[F].info {
              val originStr = origin.fold("") { origin => s", origin: $origin" }
              s"delete in ${ latency }ms, id: $id, offset: $partitionOffset, deleteTo: $deleteTo$originStr"
            }
          } yield {}
        }

        def append(partitionOffset: PartitionOffset, records: Nel[ActionRecord[Action.Append]]) = {

          val bytes = records.foldLeft(0) { case (bytes, record) => bytes + record.action.payload.size }

          val events = for {
            record <- records
            action  = record.action
            event  <- EventsFromPayload(action.payload, action.payloadType)
          } yield {
            EventRecord(record, event)
          }

          for {
            _            <- ReplicatedJournal[F].append(key, partitionOffset, roundStart, events)
            measurements <- measurements(records.size)
            _            <- Metrics[F].append(
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
              s"append in ${ latency }ms, id: $id, offset: $partitionOffset, $seqNrs$originStr"
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

      val savePointers = {

        val offsets = for {
          (topicPartition, records) <- consumerRecords
          offset = records.foldLeft(Offset.Min) { (offset, record) => record.offset max offset }
        } yield {
          (topicPartition, offset)
        }

        val offsetsToCommit = for {
          (topicPartition, offset) <- offsets
        } yield {
          val offsetAndMetadata = OffsetAndMetadata(offset + 1)
          (topicPartition, offsetAndMetadata)
        }

        val pointers = TopicPointers {
          for {
            (topicPartition, offset) <- offsets
          } yield {
            (topicPartition.partition, offset)
          }
        }

        for {
          _ <- {
            if (pointers.values.isEmpty) ().pure[F]
            else ReplicatedJournal[F].save(topic, pointers, roundStart)
          }
        } yield {
          val state1 = state.copy(pointers = state.pointers + pointers)
          (state1, offsetsToCommit)
        }
      }

      for {
        _        <- ios.parFold
        pointers <- savePointers
      } yield pointers
    }

    def ifContinue(fa: F[Either[State, Unit]]) = {
      for {
        stop   <- stopRef.get
        result <- if (stop) ().asRight[State].pure[F] else fa
      } yield result
    }

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata], state: State) = {
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
      ifContinue {
        for {
          _               <- ContextShift[F].shift
          roundStart      <- Clock[F].instant
          consumerRecords <- consumer.poll
          state           <- ifContinue {
            val records = for {
              (topicPartition, records) <- consumerRecords.values
              partition                  = topicPartition.partition
              offset                     = state.pointers.values.get(partition)
              result                     = offset.fold(records) { offset =>
                for {
                  record <- records
                  if record.offset > offset
                } yield record
              }
              if result.nonEmpty
            } yield {
              (topicPartition, result)
            }

            if (records.isEmpty) state.asLeft[Unit].pure[F]
            else for {
              timestamp        <- Clock[F].instant
              stateAndOffsets  <- round(state, records.toMap, timestamp)
              (state, offsets)  = stateAndOffsets
              state            <- commit(offsets, state)
              roundEnd         <- Clock[F].instant
              _                <- Metrics[F].round(
                latency = roundEnd diff roundStart,
                records = consumerRecords.values.foldLeft(0) { case (acc, (_, record)) => acc + record.size })
            } yield state.asLeft[Unit]
          }
        } yield state
      }
    }

    for {
      pointers <- ReplicatedJournal[F].pointers(topic)
      _        <- consumer.subscribe(topic)
      _        <- State(pointers, none).tailRecM(consume)
    } yield {}
  }


  final case class State(pointers: TopicPointers, failed: Option[Instant])


  trait Consumer[F[_]] {

    def subscribe(topic: Topic): F[Unit]

    def poll: F[ConsumerRecords[Id, Bytes]]

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

    def assignment: F[Set[TopicPartition]]
  }

  object Consumer {

    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    def apply[F[_] : Applicative](
      consumer: KafkaConsumer[F, Id, Bytes],
      pollTimeout: FiniteDuration /*TODO*/
    ): Consumer[F] = new Consumer[F] {

      def subscribe(topic: Topic) = consumer.subscribe(topic)

      def poll = consumer.poll(pollTimeout)

      def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = consumer.commit(offsets)

      def assignment = consumer.assignment
    }

    def of[F[_] : Sync : KafkaConsumerOf](
      topic: Topic,
      config: ConsumerConfig,
      pollTimeout: FiniteDuration,
      hostName: Option[HostName]): Resource[F, Consumer[F]] = {

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
        consumer <- KafkaConsumerOf[F].apply[Id, Bytes](config1)
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


  object RetryOf {

    def apply[F[_] : Sync : Timer : Log](strategy: Retry.Strategy): Retry[F] = {

      def onError(error: Throwable, details: Retry.Details) = {
        details.decision match {
          case Retry.Decision.Retry(delay) =>
            Log[F].warn(s"failed, retrying in $delay, error: $error")

          case Retry.Decision.GiveUp =>
            Log[F].error(s"failed after ${ details.retries } retries, error: $error", error)
        }
      }

      Retry(strategy, onError)
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

    def append(events: Int, bytes: Int, measurements: Measurements): F[Unit]

    def delete(measurements: Measurements): F[Unit]

    def round(latency: Long, records: Int): F[Unit]
  }

  object Metrics {

    def apply[F[_]](implicit F: Metrics[F]): Metrics[F] = F

    def empty[F[_] : Applicative]: Metrics[F] = const(Applicative[F].unit)

    def const[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def append(events: Int, bytes: Int, measurements: Measurements) = unit

      def delete(measurements: Measurements) = unit

      def round(duration: Long, records: Int) = unit
    }

    final case class Measurements(
      partition: Partition,
      replicationLatency: Long,
      deliveryLatency: Long,
      records: Int)
  }
}


