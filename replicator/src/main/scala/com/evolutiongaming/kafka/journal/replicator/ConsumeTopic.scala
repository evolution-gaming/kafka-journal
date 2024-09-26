package com.evolutiongaming.kafka.journal.replicator

import cats.data.NonEmptySet as Nes
import cats.effect.Resource
import cats.syntax.all.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Sleep, Strategy}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.RebalanceCallback.syntax.*
import com.evolutiongaming.skafka.consumer.RebalanceListener1

import scala.concurrent.duration.*

private[journal] object ConsumeTopic {

  def apply[F[_]: BracketThrowable: Sleep](
    topic: Topic,
    consumer: Resource[F, TopicConsumer[F]],
    topicFlowOf: TopicFlowOf[F],
    log: Log[F],
  ): F[Unit] = {
    for {
      random <- Random.State.fromClock[F]()
      strategy = Strategy
        .exponential(100.millis)
        .jitter(random)
        .limit(1.minute)
        .resetAfter(5.minutes)
      onError = OnError.fromLog(log)
      retry   = Retry(strategy, onError)
      result <- apply(topic, consumer, topicFlowOf, log, retry)
    } yield result
  }

  def apply[F[_]: BracketThrowable](
    topic: Topic,
    consumer: Resource[F, TopicConsumer[F]],
    topicFlowOf: TopicFlowOf[F],
    log: Log[F],
    retry: Retry[F],
  ): F[Unit] = {

    def rebalanceListenerOf(topicFlow: TopicFlow[F]): RebalanceListener1[F] = {
      new RebalanceListener1[F] {

        def onPartitionsAssigned(partitions: Nes[TopicPartition]) = {
          val partitions1 = partitions.map { _.partition }
          topicFlow.assign(partitions1).lift
        }

        def onPartitionsRevoked(partitions: Nes[TopicPartition]) = {
          val partitions1 = partitions.map { _.partition }
          topicFlow.revoke(partitions1).lift
        }
        def onPartitionsLost(partitions: Nes[TopicPartition]) = {
          val partitions1 = partitions.map { _.partition }
          topicFlow.lose(partitions1).lift
        }
      }
    }

    retry {
      (consumer, topicFlowOf(topic))
        .tupled
        .use {
          case (consumer, topicFlow) =>
            val listener = rebalanceListenerOf(topicFlow)
            for {
              _ <- consumer.subscribe(listener)
              a <- consumer
                .poll
                .mapM { records =>
                  records
                    .toNem
                    .foldMapM { records =>
                      for {
                        offsets <- topicFlow(records)
                        _ <- offsets
                          .toNem
                          .traverse { offsets =>
                            consumer
                              .commit(offsets)
                              .handleErrorWith { a => log.error(s"commit failed for $offsets: $a") }
                          }
                      } yield {}
                    }
                }
                .drain
            } yield a
        }
    }
  }
}
