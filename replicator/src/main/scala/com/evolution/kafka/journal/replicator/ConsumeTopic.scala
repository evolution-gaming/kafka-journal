package com.evolution.kafka.journal.replicator

import cats.data.NonEmptySet as Nes
import cats.effect.Resource
import cats.syntax.all.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Sleep, Strategy}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.RebalanceCallback.syntax.*
import com.evolutiongaming.skafka.consumer.{RebalanceCallback, RebalanceListener1}

import scala.concurrent.duration.*

/**
 * Consumes a topic and replicates its records into the eventual store.
 *
 * By design there is no skip or dead-letter path. The replicator must preserve the exact order and
 * completeness of the journal, so no record may ever be dropped. A record that cannot be processed
 * (for example a corrupt or unparseable one) therefore intentionally stalls replication of its
 * topic — the poll is retried with backoff and offsets are not advanced past it — until the
 * underlying cause is resolved. This stalling is the expected behaviour, not a bug: skipping the
 * record would silently diverge the replica from the source journal.
 */
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
      retry = Retry(strategy, onError)
      result <- apply(topic, consumer, topicFlowOf, retry)
    } yield result
  }

  def apply[F[_]: BracketThrowable](
    topic: Topic,
    consumer: Resource[F, TopicConsumer[F]],
    topicFlowOf: TopicFlowOf[F],
    retry: Retry[F],
  ): F[Unit] = {

    def rebalanceListenerOf(topicFlow: TopicFlow[F], topicCommit: TopicCommit[F]): RebalanceListener1[F] = {
      new RebalanceListener1[F] {

        def onPartitionsAssigned(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] = {
          val partitions1 = partitions.map { _.partition }
          topicFlow.assign(partitions1).lift >> topicCommit.onPartitionsAssigned(partitions1)
        }

        def onPartitionsRevoked(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] = {
          val partitions1 = partitions.map { _.partition }
          topicFlow.revoke(partitions1).lift >> topicCommit.onPartitionsRevoked(partitions1)
        }

        def onPartitionsLost(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] = {
          val partitions1 = partitions.map { _.partition }
          topicFlow.lose(partitions1).lift >> topicCommit.onPartitionsLost(partitions1)
        }
      }
    }

    val consumerAndFlow = for {
      consumer <- consumer
      topicFlow <- topicFlowOf(topic, consumer.commit)
    } yield (consumer, topicFlow)

    retry {
      consumerAndFlow.use {
        case (consumer, topicFlow) =>
          val listener = rebalanceListenerOf(topicFlow, consumer.commit)
          for {
            _ <- consumer.subscribe(listener)
            a <- consumer.poll.mapM { records =>
              records.toNem.traverse_ { records =>
                topicFlow(records)
              }
            }.drain
          } yield a
      }
    }
  }
}
