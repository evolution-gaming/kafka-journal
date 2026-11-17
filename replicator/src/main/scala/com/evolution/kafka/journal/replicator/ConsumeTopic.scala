package com.evolution.kafka.journal.replicator

import cats.data.{NonEmptyList as Nel, NonEmptySet as Nes, NonEmptyVector}
import cats.effect.*
import cats.effect.implicits.*
import cats.syntax.all.*
import com.evolution.kafka.journal.ConsRecord
import com.evolution.kafka.journal.util.ResourceRegistry
import com.evolution.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.ParallelHelper.*
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Sleep, Strategy}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.RebalanceCallback.syntax.*
import com.evolutiongaming.skafka.consumer.{RebalanceCallback, RebalanceListener1}

import scala.concurrent.duration.*
import scala.util.Try

/**
 * Consumes one or more topics with a single Kafka consumer and replicates their records into the
 * eventual store.
 *
 * Records and rebalance events are routed to a per-topic [[TopicFlow]] and [[TopicCommit]], which
 * are created before the topic is subscribed to.
 *
 * The topics to consume are re-read after processing each poll result; new ones are added to the
 * consumer's subscription (which triggers a rebalance). Topics are never unsubscribed.
 *
 * By design there is no skip or dead-letter path. The replicator must preserve the exact order and
 * completeness of the journal, so no record may ever be dropped. A record that cannot be processed
 * (for example a corrupt or unparseable one) therefore intentionally stalls replication of all the
 * topics of the consumer — the poll is retried with backoff and offsets are not advanced past it —
 * until the underlying cause is resolved. This stalling is the expected behaviour, not a bug:
 * skipping the record would silently diverge the replica from the source journal.
 */
private[journal] object ConsumeTopic {

  def apply[F[_]: Concurrent: Sleep](
    topics: F[Nes[Topic]],
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
      result <- apply(topics, consumer, topicFlowOf, log, retry)
    } yield result
  }

  def apply[F[_]: Concurrent](
    topics: F[Nes[Topic]],
    consumer: Resource[F, TopicConsumer[F]],
    topicFlowOf: TopicFlowOf[F],
    log: Log[F],
    retry: Retry[F],
  ): F[Unit] = {

    final case class Subscription(flow: TopicFlow[F], commit: TopicCommit[F])

    type Subscriptions = Map[Topic, Subscription]

    def rebalanceListenerOf(subscriptionsRef: Ref[F, Subscriptions]): RebalanceListener1[F] = {

      def perTopic(
        partitions: Nes[TopicPartition],
      )(
        f: (Subscription, Nes[Partition]) => RebalanceCallback[F, Unit],
      ): RebalanceCallback[F, Unit] = {
        for {
          subscriptions <- subscriptionsRef.get.lift
          _ <- partitions
            .groupBy { _.topic }
            .toSortedMap
            .toList
            .traverse_ {
              case (topic, partitions) =>
                val partitions1 = partitions.map { _.partition }
                subscriptions
                  .get(topic)
                  .fold {
                    log.warn(s"rebalance event for unknown topic $topic: ${ partitions1.mkString_(",") }").lift
                  } { subscription =>
                    f(subscription, partitions1)
                  }
            }
        } yield {}
      }

      /**
       * Partitions without an offset committed by the consumer group (e.g. a new group, as after
       * switching between per-topic and shared consumers) would be consumed from
       * `auto.offset.reset`, i.e. from the earliest offset, re-reading records which are already
       * replicated (and skipped by [[TopicFlow]]). Instead, seeks such partitions right after the
       * last replicated record.
       *
       * On failure, falls back to the default behaviour: it is slower, but still correct.
       */
      def seekToReplicated(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] = {
        def seek(partition: TopicPartition, offset: Offset): RebalanceCallback[F, Unit] =
          RebalanceCallback.seek(partition, offset)

        val committed: RebalanceCallback[F, Map[TopicPartition, OffsetAndMetadata]] =
          RebalanceCallback.committed(partitions)

        val result = for {
          committed <- committed
          subscriptions <- subscriptionsRef.get.lift
          notCommitted = partitions.toSortedSet.toVector
            .filterNot(committed.contains)
            .groupBy { _.topic }.toVector.flatMap {
              case (topic, partitions) =>
                subscriptions.get(topic).map { subscription =>
                  (topic, subscription, NonEmptyVector.fromVectorUnsafe(partitions.map { _.partition }).toNes)
                }
            }
          replicated <- notCommitted.parFoldMap1 {
            case (topic, subscription, partitions) =>
              subscription.flow.replicatedOffsets(partitions)
                .map { _.toVector.map { case (partition, offset) => (TopicPartition(topic, partition), offset) } }
          }.lift
          seeks = replicated.flatMap { case (partition, offset) => offset.inc[Try].toOption.map { (partition, _) } }
          _ <- seeks.traverse_ { case (partition, offset) => seek(partition, offset) }
          _ <- seeks.toNev.traverse_ { seeks =>
            val seeks1 = seeks.map { case (partition, offset) => s"$partition:$offset" }.mkString_(",")
            log.info(s"no committed offsets, seek to after the last replicated: $seeks1").lift
          }
        } yield ()

        result.handleErrorWith { e =>
          log.warn(s"failed to seek to the last replicated offsets, consuming from committed or reset ones: $e", e).lift
        }
      }

      new RebalanceListener1[F] {

        def onPartitionsAssigned(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] = {
          perTopic(partitions) { (subscription, partitions) =>
            subscription.flow.assign(partitions).lift >> subscription.commit.onPartitionsAssigned(partitions)
          } >> seekToReplicated(partitions)
        }

        def onPartitionsRevoked(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] = {
          perTopic(partitions) { (subscription, partitions) =>
            subscription.flow.revoke(partitions).lift >> subscription.commit.onPartitionsRevoked(partitions)
          }
        }

        def onPartitionsLost(partitions: Nes[TopicPartition]): RebalanceCallback[F, Unit] = {
          perTopic(partitions) { (subscription, partitions) =>
            subscription.flow.lose(partitions).lift >> subscription.commit.onPartitionsLost(partitions)
          }
        }
      }
    }

    def consume(
      consumer: TopicConsumer[F],
      registry: ResourceRegistry[F],
      subscriptionsRef: Ref[F, Subscriptions],
    ): F[Unit] = {

      val listener = rebalanceListenerOf(subscriptionsRef)

      // flow & commit of a topic are created before subscribing to it, so the rebalance listener
      // and record processing can always find them
      val subscribe: F[Unit] = {
        for {
          topics <- topics
          subscriptions <- subscriptionsRef.get
          _ <- topics.filterNot(subscriptions.contains).toNes.traverse_ { added =>
            for {
              added <- added.toNonEmptyList.traverse { topic =>
                registry
                  .allocate {
                    (topicFlowOf(topic), consumer.commitOf(topic)).mapN { Subscription.apply }
                  }
                  .tupleLeft(topic)
              }
              _ <- subscriptionsRef.update { _ ++ added.toList }
              _ <- log.info(s"subscribe to ${ added.map { case (topic, _) => topic }.mkString_(",") }")
              _ <- consumer.subscribe(topics, listener)
            } yield {}
          }
        } yield {}
      }

      def replicate(topic: Topic, records: Map[TopicPartition, Nel[ConsRecord]]): F[Unit] = {
        records
          .map { case (topicPartition, records) => (topicPartition.partition, records) }
          .toNem
          .traverse_ { records =>
            for {
              subscriptions <- subscriptionsRef.get
              subscription <- subscriptions
                .get(topic)
                .liftTo[F] {
                  // must never happen; failing instead of skipping records, which would never be replicated
                  new IllegalStateException(s"polled records of not subscribed topic $topic")
                }
              offsets <- subscription.flow(records)
              _ <- offsets.toNem.traverse_ { offsets =>
                subscription
                  .commit(offsets)
                  .handleErrorWith { e => log.error(s"commit failed for $topic $offsets: $e", e) }
              }
            } yield {}
          }
      }

      for {
        _ <- subscribe
        _ <- consumer.poll.mapM { records =>
          val records1 = records.groupBy { case (topicPartition, _) => topicPartition.topic }.toVector
          records1.parFoldMap1 { case (topic, records) => replicate(topic, records) } >> subscribe
        }.drain
      } yield ()
    }

    retry {
      // registry is released before the consumer, so flows & commits are released before the consumer
      // is closed
      (consumer, ResourceRegistry.make[F])
        .tupled
        .use {
          case (consumer, registry) =>
            for {
              subscriptionsRef <- Ref.of[F, Subscriptions](Map.empty)
              a <- consume(consumer, registry, subscriptionsRef)
            } yield a
        }
    }
  }
}
