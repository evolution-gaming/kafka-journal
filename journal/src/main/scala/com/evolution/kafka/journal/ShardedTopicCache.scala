package com.evolution.kafka.journal

import cats.data.NonEmptySet
import cats.effect.*
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.{Monad, MonadThrow, Order, Parallel}
import com.evolution.kafka.journal.HeadCache.Eventual
import com.evolution.kafka.journal.PartitionCache.Result
import com.evolution.kafka.journal.conversions.ConsRecordToActionHeader
import com.evolution.kafka.journal.util.SkafkaHelper.*
import com.evolution.kafka.journal.util.StreamHelper.*
import com.evolutiongaming.catshelper.{FromTry, Log, MeasureDuration}
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.Retry.implicits.*
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  ConsumerConfig,
  ConsumerRecords,
  RebalanceCallback,
  RebalanceListener1,
}
import com.evolutiongaming.skafka.{Offset, Partition, Topic, TopicPartition}
import com.evolutiongaming.sstream.Stream

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*

/**
 * Head cache of a single topic sharded between the pods of an application through a Kafka consumer
 * group.
 */
private[journal] final class ShardedTopicCache[F[_]: MonadThrow: MeasureDuration] private (
  topic: Topic,
  ownedPartitions: Ref[F, Map[Partition, ShardedTopicCache.Owned[F]]],
  transport: HeadCacheTransport[F],
  metrics: Option[HeadCache.Metrics[F]],
  log: Log[F],
) {
  import ShardedTopicCache.*

  def get(id: String, partition: Partition, offset: Offset): F[Option[HeadInfo]] = {
    for {
      duration <- MeasureDuration[F].start
      owned <- ownedPartitions.get
      answer <- owned.get(partition) match {
        case Some(owned) => local(owned.cache, id, offset)
        case None => remote(Key(id = id, topic = topic), partition, offset)
      }
      duration <- duration
      _ <- metrics.foldMapM { _.get(topic, duration, answer.result, answer.now) }
    } yield answer.headInfo
  }

  private def remote(key: Key, partition: Partition, offset: Offset): F[Answer] = {
    transport.getRemote(key, partition, offset).map {
      case a @ Some(_) => Answer(a, "remote", now = false)
      case None => Answer(none, "remote_none", now = false)
    }.handleErrorWith { e =>
      log
        .warn(s"request failed, partition=$partition, id=${ key.id }", e)
        .as(Answer(none, "remote_failure", now = false))
    }
  }
}

private[journal] object ShardedTopicCache {

  /**
   * Creates the cache and starts consuming the topic in the background.
   */
  def make[F[_]: Async: Parallel: MeasureDuration](
    topic: Topic,
    eventual: Eventual[F],
    consumer: Resource[F, Consumer[F]],
    transport: HeadCacheTransport[F],
    config: HeadCacheConfig,
    pointerTimeout: FiniteDuration,
    consRecordToActionHeader: ConsRecordToActionHeader[F],
    metrics: Option[HeadCache.Metrics[F]],
    log: Log[F],
  ): Resource[F, ShardedTopicCache[F]] = {
    for {
      ownedRef <- Ref[F].of(Map.empty[Partition, Owned[F]]).toResource
      revokeAll = ownedRef.getAndSet(Map.empty).flatMap { owned => releaseOwned(owned.toVector, log) }
      _ <- Resource.onFinalize(revokeAll)
      listener = new Listener(
        topic = topic,
        eventual = eventual,
        transport = transport,
        config = config,
        pointerTimeout = pointerTimeout,
        ownedRef = ownedRef,
        log = log,
      )

      // handler of one poll return
      handle = (records: ConsumerRecords[String, Unit]) => {
        for {
          owned <- ownedRef.get
          now <- Clock[F].realTime
          diffs <- records.values.toVector.parFlatTraverse {
            case (topicPartition, records) =>
              owned.get(topicPartition.partition).fold {
                // revoked partition, see `Listener.onRevoked`
                Vector.empty[Long].pure[F]
              } { owned =>
                records.traverse { record =>
                  record.key.traverseFilter { key =>
                    consRecordToActionHeader.apply(record).map { header =>
                      PartitionCache.Record.Data(key.value, header)
                    }.value
                  }.map { data =>
                    PartitionCache.Record(record.offset, data)
                  }
                }.flatMap { records =>
                  owned.cache.add(records)
                }.map { diff =>
                  diff.map(_.value).toVector
                }.recover { case ReleasedError => Vector.empty }
              }
          }
          _ <- metrics.traverse_ { metrics =>
            avg(diffs).traverse_ { diff =>
              val timestamp = records.values.values
                .flatMap(_.iterator.flatMap(_.timestampAndType.map(_.timestamp.toEpochMilli)))
                .minOption
              timestamp.traverse_ { timestamp =>
                metrics.consumer(topic, age = now - timestamp.millis, diff = diff)
              }
            }
          }
        } yield {}
      }

      // background job, retrieves pointers from Cassandra and removes replicated offsets from cache
      remove = for {
        owned <- ownedRef.get
        diffs <- owned.toVector.parFlatTraverse {
          case (partition, owned) =>
            eventual.pointer(topic, partition).flatMap {
              case Some(offset) => owned.cache.remove(offset).map(_.map(_.value).toVector)
              case None => Vector.empty[Long].pure[F]
            }.recover { case ReleasedError => Vector.empty }
        }
        _ <- metrics.traverse_ { metrics =>
          avg(diffs).traverse_ { diff =>
            metrics.storage(topic, diff)
          }
        }
      } yield ()

      random <- Random.State.fromClock[F]().toResource

      _ <- {
        val retry = {
          // slower than in TopicCache: every new consumer rebalances the whole group
          val strategy = Strategy.exponential(100.millis).jitter(random).cap(10.seconds).resetAfter(1.minute)
          Retry(strategy, OnError.fromLog(log.prefixed("consuming")))
        }
        val records = for {
          _ <- Stream.around(retry.toFunctionK)
          // the consumer is closed first, then the partitions it owned are released
          _ <- Resource.onFinalize(revokeAll).toStream
          consumer <- consumer.toStream
          _ <- consumer.subscribe(topic, listener).toStream
          records <- Stream.repeat(consumer.poll) if records.values.nonEmpty
        } yield records
        records
          .foreach(handle)
          .onError { case e => log.error(s"consuming failed", e) }
          .background
      }

      _ <- {
        val strategy = Strategy.exponential(10.millis).cap(3.seconds).jitter(random)
        Temporal[F].sleep(config.removeInterval)
          .productR(remove)
          .retry(strategy)
          .handleErrorWith { e => log.error(s"remove failed, error: $e", e) }
          .foreverM[Unit].background
      }

      _ <- metrics.traverse_ { metrics =>
        val result = for {
          _ <- Temporal[F].sleep(1.minute)
          owned <- ownedRef.get
          meters <- owned.values.toVector.foldMapM { owned =>
            owned.cache.meters.recover { case ReleasedError => PartitionCache.Meters.Empty }
          }
          _ <- metrics.meters(topic, entries = meters.entries, listeners = meters.listeners)
        } yield ()
        result
          .handleErrorWith { e => log.error(s"metrics.meters failed, error: $e", e) }
          .foreverM[Unit].background.void
      }
    } yield new ShardedTopicCache(topic, ownedRef, transport, metrics, log)
  }

  private final case class Owned[F[_]](cache: PartitionCache[F], release: F[Unit])

  private final case class Answer(headInfo: Option[HeadInfo], result: String, now: Boolean)

  private def avg(values: Vector[Long]): Option[Long] =
    if (values.isEmpty) none else (values.sum / values.size).some

  private def local[F[_]: MonadThrow](cache: PartitionCache[F], id: String, offset: Offset): F[Answer] =
    cache.get(id, offset).flatMap { result =>
      val now = result match {
        case _: Result.Now[F] => true
        case _: Result.Later[F] => false
      }
      result.toNow.map {
        case a: Result.Now.Value[F] => Answer(a.value.some, "value", now)
        case _: Result.Now.Ahead[F] => Answer(HeadInfo.empty.some, "ahead", now)
        case _: Result.Now.Limited[F] => Answer(none, "limited", now)
        case _: Result.Now.Timeout[F] => Answer(none, "timeout", now)
      }
    }.recover {
      case ReleasedError => Answer(none, "released", now = false)
    }

  private final class LocalHandle[F[_]: MonadThrow](cache: PartitionCache[F]) extends PartitionHeadCacheHandle[F] {
    def get(offset: Offset, id: String): F[Option[HeadInfo]] = local(cache, id, offset).map { _.headInfo }
  }

  private def releaseOwned[F[_]: MonadThrow: Parallel](owned: Vector[(Partition, Owned[F])], log: Log[F]): F[Unit] = {
    owned.parTraverse_ {
      case (partition, owned) =>
        owned.release.handleErrorWith { e =>
          log.error(s"release failed, partition=$partition", e)
        }
    }
  }

  final class Listener[F[_]: Async: Parallel] private[ShardedTopicCache] (
    topic: Topic,
    eventual: Eventual[F],
    transport: HeadCacheTransport[F],
    config: HeadCacheConfig,
    pointerTimeout: FiniteDuration,
    ownedRef: Ref[F, Map[Partition, Owned[F]]],
    log: Log[F],
  ) {

    def onAssigned(partitions: NonEmptySet[Partition]): F[Map[Partition, Option[Offset]]] = {
      for {
        results <- partitions.toSortedSet.toVector.parTraverse { partition =>
          allocate(partition).attempt
        }
        allocated = results.collect { case Right(a) => a }
        // fail fast if one of partitions failed to allocate a cache
        _ <- results.collectFirst { case Left(e) => e }.traverse_ { e =>
          releaseOwned(allocated.map { case (partition, owned, _) => (partition, owned) }, log) *>
            e.raiseError[F, Unit]
        }
        previous <- ownedRef.modify { owned =>
          val previous = allocated.flatMap { case (partition, _, _) => owned.get(partition).tupleLeft(partition) }
          val assigned = allocated.map { case (partition, owned, _) => (partition, owned) }
          (owned ++ assigned, previous)
        }
        _ <- releaseOwned(previous, log)
        _ <- log.info {
          val offsets = allocated.map { case (partition, _, from) =>
            s"${ partition.value }:${ from.fold("beginning")(_.value.toString) }"
          }.mkString(",")
          s"partitions assigned: $offsets"
        }
      } yield allocated.map { case (partition, _, from) => (partition, from) }.toMap
    }

    def onRevoked(partitions: NonEmptySet[Partition]): F[Unit] = {
      for {
        revoked <- ownedRef.modify { owned =>
          val (revoked, kept) = owned.partition { case (p, _) =>
            partitions.contains(p)
          }
          (kept, revoked.toVector)
        }
        _ <- releaseOwned(revoked, log)
        _ <- log.info(s"partitions revoked: ${ partitions.toSortedSet.map(_.value).mkString(",") }")
      } yield ()
    }

    private def allocate(partition: Partition): F[(Partition, Owned[F], Option[Offset])] = {
      val owned = for {
        cache <- PartitionCache.make[F](
          maxSize = config.partition.maxSize,
          dropUponLimit = config.partition.dropUponLimit,
          timeout = config.timeout,
        )
        pointer <- eventual.pointer(topic, partition).timeout(pointerTimeout).toResource
        _ <- pointer.traverse_ { offset => cache.remove(offset) }.toResource
        _ <- transport.serve(topic, partition, new LocalHandle(cache))
        from <- pointer.traverse { _.inc[F] }.toResource
      } yield (cache, from)

      owned.allocated.map { case ((cache, from), release) => (partition, Owned(cache, release), from) }
    }
  }

  trait Consumer[F[_]] {
    def subscribe(topic: Topic, listener: Listener[F]): F[Unit]

    def poll: F[ConsumerRecords[String, Unit]]
  }

  object Consumer {
    private val AssignmentStrategy: String = "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"

    def apply[F[_]](consumer: KafkaConsumer[F, String, Unit], pollTimeout: FiniteDuration): Consumer[F] = {
      new Main(consumer, pollTimeout)
    }

    def make[F[_]: Monad: FromTry](
      kafkaConsumerOf: KafkaConsumerOf[F],
      config: ConsumerConfig,
      groupId: String,
      pollTimeout: FiniteDuration = 10.millis,
    ): Resource[F, Consumer[F]] = {
      val config1 = config.copy(
        groupId = groupId.some,
        autoCommit = false,
        autoOffsetReset = AutoOffsetReset.None,
        partitionAssignmentStrategy = AssignmentStrategy,
      )
      kafkaConsumerOf.apply[String, Unit](config1).map { consumer => apply(consumer, pollTimeout) }
    }

    private final class Main[F[_]](
      consumer: KafkaConsumer[F, String, Unit],
      pollTimeout: FiniteDuration,
    ) extends Consumer[F] {
      def subscribe(topic: Topic, listener: Listener[F]): F[Unit] =
        consumer.subscribe(topic, new Rebalance(topic, listener))

      val poll: F[ConsumerRecords[String, Unit]] = consumer.poll(pollTimeout)
    }

    private final class Rebalance[F[_]](topic: Topic, listener: Listener[F]) extends RebalanceListener1[F] {
      private val api = RebalanceCallback.api[F]
      private val unit = api.pure(())

      def onPartitionsAssigned(topicPartitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] = {
        partitionsOf(topicPartitions).fold[RebalanceCallback[F, Unit]](unit) { partitions =>
          for {
            offsets <- api.lift(listener.onAssigned(partitions))
            _ <- offsets.toVector.collect {
              case (partition, Some(offset)) => (partition, offset)
            }.traverse_ {
              case (partition, offset) =>
                api.seek(TopicPartition(topic = topic, partition = partition), offset)
            }
            _ <- offsets.toVector.collect {
              case (partition, None) => TopicPartition(topic = topic, partition = partition)
            }.toNonEmptySet.traverse_ { partitions =>
              api.seekToBeginning(partitions)
            }
          } yield ()
        }
      }

      def onPartitionsRevoked(topicPartitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] =
        revoke(topicPartitions)

      def onPartitionsLost(topicPartitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] =
        revoke(topicPartitions)

      private def partitionsOf(topicPartitions: NonEmptySet[TopicPartition]): Option[NonEmptySet[Partition]] =
        NonEmptySet.fromSet(topicPartitions.collect { case a if a.topic == topic => a.partition })

      private def revoke(topicPartitions: NonEmptySet[TopicPartition]): RebalanceCallback[F, Unit] =
        partitionsOf(topicPartitions).fold[RebalanceCallback[F, Unit]](unit) { partitions =>
          api.lift(listener.onRevoked(partitions))
        }

    }
  }

  private implicit class vectorOps[A](private val self: Vector[A]) extends AnyVal {
    def toNonEmptySet(
      implicit
      O: Order[A],
    ): Option[NonEmptySet[A]] = {
      NonEmptySet.fromSet(SortedSet.from(self)(Order[A].toOrdering))
    }
  }
}
