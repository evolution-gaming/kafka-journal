package com.evolutiongaming.kafka.journal.replicator


import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect.{Resource, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.journal.util.CollectionHelper._
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import com.evolutiongaming.skafka._
import com.evolutiongaming.skafka.consumer.{Consumer => _, _}

import scala.concurrent.duration._

object ConsumeTopic {

  def apply[F[_] : BracketThrowable : Timer](
    topic: Topic,
    consumer: Resource[F, TopicConsumer[F]],
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
      retry <- retry(log)
      result <- apply(topic, consumer, topicFlowOf, log, retry)
    } yield result
  }


  def apply[F[_] : BracketThrowable](
    topic: Topic,
    consumer: Resource[F, TopicConsumer[F]],
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
              records
                .toNem
                .foldMapM { records =>
                  for {
                    offsets <- topicFlow(records)
                    _       <- offsets.toNem.traverse { offsets => commit(offsets) }
                  } yield {}
                }
            }
            .drain

          for {
            _      <- consumer.subscribe(listener)
            result <- consume
          } yield result
        }
    }
  }
}