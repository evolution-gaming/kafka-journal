package com.evolutiongaming.kafka.journal.replicator

import cats.Monad
import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect.{Resource, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.journal.{ConsRecords, KafkaConsumer}
import com.evolutiongaming.kafka.journal.util.CollectionHelper._
import com.evolutiongaming.skafka.consumer.{Consumer => _, _}
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, Partition, Topic, TopicPartition}
import com.evolutiongaming.sstream.Stream
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import scodec.bits.ByteVector

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
        random   <- Random.State.fromClock[F]()
        strategy  = strategyOf(random)
        onError   = OnError.fromLog(log)
        retry     = Retry(strategy, onError)
      } yield retry
    }

    for {
      retry  <- retry(log)
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
              for {
                offsets <- records.values.toNem.foldMapM({ records => topicFlow(records) })
                result  <- offsets.toNem.foldMapM { offsets => commit(offsets) }
              } yield result
            }
            .drain

          for {
            _      <- consumer.subscribe(listener)
            result <- consume
          } yield result
        }
    }
  }


  trait Consumer[F[_]] {

    def subscribe(listener: RebalanceListener[F]): F[Unit]

    def poll: Stream[F, ConsRecords]

    def commit(offsets: Nem[Partition, Offset]): F[Unit]
  }

  object Consumer {

    def apply[F[_] : Monad](
      topic: Topic,
      pollTimeout: FiniteDuration,
      metadata: String,
      consumer: KafkaConsumer[F, String, ByteVector],
    ): Consumer[F] = {

      new Consumer[F] {

        def subscribe(listener: RebalanceListener[F]) = {
          consumer.subscribe(topic, listener.some)
        }

        val poll = Stream.repeat(consumer.poll(pollTimeout))

        def commit(offsets: Nem[Partition, Offset]) = {
          val offsets1 = offsets.mapKV { (partition, offset) =>
            val offset1 = OffsetAndMetadata(offset, metadata)
            val partition1 = TopicPartition(topic, partition)
            (partition1, offset1)
          }
          consumer.commit(offsets1)
        }
      }
    }
  }
}