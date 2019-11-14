package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect.{Resource, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.{BracketThrowable, Log, LogOf}
import com.evolutiongaming.kafka.journal.{ConsRecord, ConsRecords, KafkaConsumer}
import com.evolutiongaming.kafka.journal.util.CollectionHelper._
import com.evolutiongaming.skafka.consumer.{Consumer => _, _}
import com.evolutiongaming.skafka.{Offset, OffsetAndMetadata, Partition, Topic, TopicPartition}
import com.evolutiongaming.sstream.Stream
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import scodec.bits.ByteVector

import scala.concurrent.duration._

object SubscriptionFlow {

  type Records = Nem[TopicPartition, Nel[ConsRecord]]


  def apply[F[_] : BracketThrowable : LogOf : Timer](
    topic: Topic,
    consumer: Resource[F, Consumer[F]],
    topicFlowOf: TopicFlowOf[F]
  ): Stream[F, Unit] = {

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

    def log = {
      for {
        log <- LogOf[F].apply(getClass)
      } yield {
        log.prefixed(topic)
      }
    }

    for {
      log    <- Stream.lift(log)
      retry  <- Stream.lift(retry(log))
      result <- apply(topic, consumer, topicFlowOf, retry)
    } yield result
  }

  def apply[F[_] : BracketThrowable](
    topic: Topic,
    consumer: Resource[F, Consumer[F]],
    topicFlowOf: TopicFlowOf[F],
    retry: Retry[F],
  ): Stream[F, Unit] = {

    def rebalanceListenerOf(topicFlow: TopicFlow[F]) = {
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

    for {
      _         <- Stream.around(retry.toFunctionK)
      consumer  <- Stream.fromResource(consumer)
      topicFlow  = topicFlowOf(topic, consumer)
      topicFlow <- Stream.fromResource(topicFlow)
      listener   = rebalanceListenerOf(topicFlow)
      subscribe  = consumer.subscribe(listener)
      _         <- Stream.lift(subscribe)
      records   <- Stream.repeat(consumer.poll)
      records   <- Stream[F].apply(records.values.toNem)
      offsets   <- Stream.lift(topicFlow(records))
      offsets   <- Stream[F].apply(offsets.toNem)
      _         <- Stream.lift(consumer.commit(offsets))
    } yield {}
  }


  trait Consumer[F[_]] {

    def subscribe(listener: RebalanceListener[F]): F[Unit]

    def poll: F[ConsRecords]

    def commit(offsets: Nem[Partition, Offset]): F[Unit]
  }

  object Consumer {

    def apply[F[_]](
      topic: Topic,
      pollTimeout: FiniteDuration,
      metadata: String,
      consumer: KafkaConsumer[F, String, ByteVector],
    ): Consumer[F] = {

      new Consumer[F] {

        def subscribe(listener: RebalanceListener[F]) = {
          consumer.subscribe(topic, listener.some)
        }

        val poll = consumer.poll(pollTimeout)

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