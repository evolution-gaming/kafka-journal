package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.Monad
import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect.concurrent.Ref
import cats.effect.{Clock, Resource, Sync, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.journal.util.CollectionHelper._
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.kafka.journal.{ConsRecords, KafkaConsumer}
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import com.evolutiongaming.skafka.consumer.{Consumer => _, _}
import com.evolutiongaming.skafka._
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import scala.collection.immutable.SortedMap
import scala.concurrent.duration._

object ConsumeTopic {

  def apply[F[_] : BracketThrowable : Timer](
    topic: Topic,
    consumer: Resource[F, Consumer[F]],
    topicFlowOf: TopicFlowOf[F],
    log: Log[F]
  ): F[Unit] = {

    def retry(log: Log[F]) = {

      def strategyOf(random: Random.State) = {
        Strategy
          .fullJitter(100.millis, random)
          .limit(1.minute)
          .resetAfter(5.minutes)
      }

      for {
        random <- Random.State.fromClock[F]()
        strategy = strategyOf(random)
        onError = OnError.fromLog(log)
        retry = Retry(strategy, onError)
      } yield retry
    }

    for {
      retry <- retry(log)
      result <- apply(topic, consumer, topicFlowOf, log, retry)
    } yield result
  }


  def apply[F[_] : BracketThrowable](
    topic: Topic,
    consumer: Resource[F, Consumer[F]],
    topicFlowOf: TopicFlowOf[F],
    log: Log[F],
    retry: Retry[F]
  ): F[Unit] = {

    def rebalanceListenerOf(topicFlow: TopicFlow[F]): RebalanceListener[F] = {
      new RebalanceListener[F] {

        def onPartitionsAssigned(partitions: Nel[TopicPartition]) = {
          val partitions1 = partitions.map { _.partition }
          topicFlow.assign(partitions1)
        }

        def onPartitionsRevoked(partitions: Nel[TopicPartition]) = {
          val partitions1 = partitions.map { _.partition }
          topicFlow.revoke(partitions1)
        }
      }
    }

    retry {
      (consumer, topicFlowOf(topic))
        .tupled
        .use { case (consumer, topicFlow) =>
          val listener = rebalanceListenerOf(topicFlow)

          def commit(offsets: Nem[Partition, Offset]) = {
            consumer
              .commit(offsets)
              .handleErrorWith { a => log.error(s"commit failed for $offsets: $a") }
          }

          val consume = consumer
            .poll
            .mapM { records =>

              val records1 = records
                .values
                .map { case (topicPartition, records) => (topicPartition.partition, records) }

              for {
                offsets <- records1.toNem.foldMapM { records => topicFlow(records) }
                result  <- offsets.toNem.foldMapM { offsets => commit(offsets) }
              } yield result
            }
            .drain

          for {
            _ <- consumer.subscribe(listener)
            result <- consume
          } yield result
        }
    }
  }


  trait Commit[F[_]] {

    def apply(offsets: Nem[Partition, Offset]): F[Unit]
  }

  object Commit {

    def apply[F[_]](
      topic: Topic,
      metadata: String,
      consumer: KafkaConsumer[F, _, _],
    ): Commit[F] = {
      (offsets: Nem[Partition, Offset]) => {
        val offsets1 = offsets.mapKV { (partition, offset) =>
          val offset1 = OffsetAndMetadata(offset, metadata)
          val partition1 = TopicPartition(topic, partition)
          (partition1, offset1)
        }
        consumer.commit(offsets1)
      }
    }

    def delayed[F[_] : Sync : Clock](
      delay: FiniteDuration,
      commit: Commit[F]
    ): F[Commit[F]] = {

      case class State(until: Instant, offsets: SortedMap[Partition, Offset] = SortedMap.empty)

      for {
        timestamp <- Clock[F].instant
        stateRef  <- Ref[F].of(State(timestamp + delay))
      } yield {
        new Commit[F] {
          def apply(offsets: Nem[Partition, Offset]) = {

            def apply(state: State, timestamp: Instant) = {
              val offsets1 = state.offsets ++ offsets.toSortedMap
              if (state.until <= timestamp) {
                offsets1
                  .toNem
                  .foldMapM { offsets => commit(offsets) }
                  .as(State(timestamp + delay))
              } else {
                state
                  .copy(offsets = offsets1)
                  .pure[F]
              }
            }

            for {
              timestamp <- Clock[F].instant
              state     <- stateRef.get
              state     <- apply(state, timestamp)
              _         <- stateRef.set(state)
            } yield {}
          }
        }
      }
    }
  }


  trait Consumer[F[_]] {

    def subscribe(listener: RebalanceListener[F]): F[Unit]

    // TODO return same topic values
    def poll: Stream[F, ConsRecords]

    def commit: Commit[F]
  }

  object Consumer {

    def apply[F[_] : Monad](
      topic: Topic,
      pollTimeout: FiniteDuration,
      commit: Commit[F],
      consumer: KafkaConsumer[F, String, ByteVector],
    ): Consumer[F] = {

      val commit1 = commit

      new Consumer[F] {

        def subscribe(listener: RebalanceListener[F]) = {
          consumer.subscribe(topic, listener.some)
        }

        val poll = Stream.repeat(consumer.poll(pollTimeout))

        def commit = commit1
      }
    }
  }
}