package com.evolutiongaming.kafka.journal.replicator

import cats.Applicative
import cats.data.{NonEmptyMap as Nem, NonEmptySet}
import cats.effect.*
import cats.syntax.all.*
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.KafkaConsumer
import com.evolutiongaming.kafka.journal.replicator.commit.AsyncPeriodicTopicCommit
import com.evolutiongaming.kafka.journal.util.TemporalHelper.*
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.RebalanceCallback

import java.time.Instant
import scala.annotation.nowarn
import scala.collection.immutable.SortedMap
import scala.concurrent.duration.*

/**
 * Kafka partition offset commit logic for replicator internal topic processing API
 * ([[TopicConsumer]], [[ConsumeTopic]]).
 *
 * @tparam F
 *   effect type
 */
private[journal] trait TopicCommit[F[_]] {
// journal-private because the places where it is used are journal-private - TopicConsumer, ConsumeTopic

  /**
   * Mark offsets for commit.
   *
   * The method should be called after each processing step, i.e., after each consumer poll result
   * processing.
   *
   * Please note that offsets passed here should follow the same logic as for Java consumer commit
   * methods: if offset N is known to be processed, offset N+1 should be committed - offset from
   * which you want to start your processing next time.
   *
   * Depending on the underlying implementation, this method might execute commit synchronously with
   * the call or schedule it for later.
   *
   * @param offsets
   *   partition offsets to commit
   */
  def apply(offsets: Nem[Partition, Offset]): F[Unit]

  /**
   * Notify topic commit logic about new partitions assigned during rebalance - called in
   * [[com.evolutiongaming.skafka.consumer.RebalanceListener1.onPartitionsAssigned]].
   *
   * @param partitions
   *   assigned partitions
   *
   * @return
   *   [[RebalanceCallback]]
   */
  def onPartitionsAssigned(@nowarn("cat=unused") partitions: NonEmptySet[Partition]): RebalanceCallback[F, Unit] =
    RebalanceCallback.empty

  /**
   * Notify topic commit logic about partitions revoked during rebalance - called in
   * [[com.evolutiongaming.skafka.consumer.RebalanceListener1.onPartitionsRevoked]].
   *
   * Could be used to commit offsets for revoked partitions using [[RebalanceCallback.commit]] in
   * order not to lose progress during rebalance.
   *
   * @param partitions
   *   revoked partitions
   *
   * @return
   *   [[RebalanceCallback]]
   */
  def onPartitionsRevoked(@nowarn("cat=unused") partitions: NonEmptySet[Partition]): RebalanceCallback[F, Unit] =
    RebalanceCallback.empty

  /**
   * Notify topic commit logic about partitions lost - called in
   * [[com.evolutiongaming.skafka.consumer.RebalanceListener1.onPartitionsLost]].
   *
   * When partitions are lost as opposed to being revoked:
   * [[org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsLost(java.util.Collection)]]
   *
   * Usually nothing could be done in this case apart from removing the lost partitions from
   * internal state, if there is any. As another consumer might already handle the lost partitions
   * by this point, it is highly recommended not to try to commit offsets for those.
   *
   * @param partitions
   *   lost partitions
   *
   * @return
   *   [[RebalanceCallback]]
   */
  def onPartitionsLost(@nowarn("cat=unused") partitions: NonEmptySet[Partition]): RebalanceCallback[F, Unit] =
    RebalanceCallback.empty
}

private[journal] object TopicCommit {

  def empty[F[_]: Applicative]: TopicCommit[F] = new EmptyTopicCommit[F]

  @deprecated("renamed to `sync`, use it instead", since = "4.2.0")
  def apply[F[_]](
    topic: Topic,
    metadata: String,
    consumer: KafkaConsumer[F, ?, ?],
  ): TopicCommit[F] = {
    sync[F](
      topic = topic,
      commitMetadata = metadata,
      consumer = consumer,
    )
  }

  /**
   * [[TopicCommit]] which performs synchronous blocking commit on each offset mark call.
   *
   * @param topic
   *   processed topic name
   * @param commitMetadata
   *   metadata to pass to each consumer commit call
   * @param consumer
   *   [[KafkaConsumer]] to call commit on
   * @tparam F
   *   effect type
   */
  def sync[F[_]](
    topic: Topic,
    commitMetadata: String,
    consumer: KafkaConsumer[F, ?, ?],
  ): TopicCommit[F] = new SyncTopicCommit[F](
    topic = topic,
    commitMetadata = commitMetadata,
    consumer = consumer,
  )

  @deprecated(
    "use `asyncPeriodic` instead - the delayed impl blocks the poll-loop on commit and " +
      "looses progress on rebalance and shutdown",
    since = "4.2.0",
  )
  def delayed[F[_]: Concurrent: Clock](
    delay: FiniteDuration,
    commit: TopicCommit[F],
  ): F[TopicCommit[F]] = {

    case class State(until: Instant, offsets: SortedMap[Partition, Offset] = SortedMap.empty)

    for {
      timestamp <- Clock[F].instant
      stateRef <- Ref[F].of(State(timestamp + delay))
    } yield {
      new TopicCommit[F] {
        def apply(offsets: Nem[Partition, Offset]): F[Unit] = {

          def apply(state: State, timestamp: Instant): F[State] = {
            val offsets1 = state.offsets ++ offsets.toSortedMap
            if (state.until <= timestamp) {
              offsets1
                .toNem()
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
            state <- stateRef.get
            state <- apply(state, timestamp)
            _ <- stateRef.set(state)
          } yield {}
        }
      }
    }
  }

  /**
   * [[TopicCommit]] which commits progress periodically using an async commit method, without
   * blocking the main poll loop.
   *
   * Properly handles rebalance events - when partitions are revoked, it performs a synchronous
   * commit for revoked partitions in order not to lose the progress. Since the partition revoke
   * callback is also called on consumer shutdown, the same logic saves the progress on app
   * shutdown.
   *
   * @param topic
   *   processed topic name
   * @param commitMetadata
   *   metadata to pass to each consumer commit call
   * @param commitPeriod
   *   delay between periodic async commit calls
   * @param consumer
   *   [[KafkaConsumer]] to call commit on
   * @param log
   *   logger instance. The logic here doesn't add anything to log statements which can identify the
   *   topic or particular consumer instance. If you have more than one topic-processor in the app,
   *   add that information to the logger before passing it here.
   * @tparam F
   *   effect type
   * @return
   *   implementation wrapped in Resource - don't forget to release it after use!
   */
  def asyncPeriodic[F[_]: Temporal](
    topic: Topic,
    commitMetadata: String,
    commitPeriod: FiniteDuration,
    consumer: KafkaConsumer[F, ?, ?],
    log: Log[F],
  ): Resource[F, TopicCommit[F]] =
    AsyncPeriodicTopicCommit.make(
      topic = topic,
      commitMetadata = commitMetadata,
      commitPeriod = commitPeriod,
      consumer = consumer,
      log = log,
    )

  private final class EmptyTopicCommit[F[_]: Applicative] extends TopicCommit[F] {
    override def apply(offsets: Nem[Partition, Offset]): F[Unit] = ().pure
  }

  private final class SyncTopicCommit[F[_]](
    topic: Topic,
    commitMetadata: String,
    consumer: KafkaConsumer[F, ?, ?],
  ) extends TopicCommit[F] {
    override def apply(offsets: Nem[Partition, Offset]): F[Unit] = {
      val offsets1 = offsets.mapKV { (partition, offset) =>
        val offset1 = OffsetAndMetadata(offset, commitMetadata)
        val partition1 = TopicPartition(topic, partition)
        (partition1, offset1)
      }
      consumer.commit(offsets1)
    }
  }
}
