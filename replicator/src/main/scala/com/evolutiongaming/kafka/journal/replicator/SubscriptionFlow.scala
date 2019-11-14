package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect.{Resource, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.{BracketThrowable, Log, LogOf}
import com.evolutiongaming.kafka.journal.{ConsRecord, ConsRecords, KafkaConsumer}
import com.evolutiongaming.kafka.journal.util.CollectionHelper._
import com.evolutiongaming.skafka.consumer.{Consumer => _, _}
import com.evolutiongaming.skafka.{OffsetAndMetadata, Topic, TopicPartition}
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
  ): Stream[F, Records] = {

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
      log     <- Stream.lift(log)
      retry   <- Stream.lift(retry(log))
      records <- apply(topic, consumer, topicFlowOf, retry)
    } yield records
  }

  def apply[F[_] : BracketThrowable](
    topic: Topic,
    consumer: Resource[F, Consumer[F]],
    topicFlowOf: TopicFlowOf[F],
    retry: Retry[F],
  ): Stream[F, Records] = {

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
      subscribe  = consumer.subscribe(topic, listener)
      _         <- Stream.lift(subscribe)
      records   <- Stream.repeat(consumer.poll)
      records   <- Stream[F].apply(records.values.toNem)
      _         <- Stream.lift(topicFlow(records))
    } yield records
  }


  trait Consumer[F[_]] {

    def subscribe(topic: Topic, listener: RebalanceListener[F]): F[Unit]

    def poll: F[ConsRecords]

    // TODO not pass topicPartition, as topic is constant
    def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]): F[Unit]
  }

  object Consumer {

    def apply[F[_]](
      consumer: KafkaConsumer[F, String, ByteVector],
      pollTimeout: FiniteDuration
    ): Consumer[F] = {

      new Consumer[F] {

        def subscribe(topic: Topic, listener: RebalanceListener[F]) = {
          consumer.subscribe(topic, listener.some)
        }

        val poll = consumer.poll(pollTimeout)

        def commit(offsets: Nem[TopicPartition, OffsetAndMetadata]) = {
          consumer.commit(offsets)
        }
      }
    }
  }
}