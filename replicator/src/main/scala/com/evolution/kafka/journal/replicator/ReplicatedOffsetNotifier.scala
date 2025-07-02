package com.evolution.kafka.journal.replicator

import cats.Applicative
import com.evolutiongaming.skafka.{Offset, TopicPartition}

/**
 * Interface for receiving timely notifications about [[Replicator]] topic replication progress.
 *
 * @tparam F
 *   effect type
 */
trait ReplicatedOffsetNotifier[F[_]] {

  /**
   * This method's effect is evaluated when a topic-partition offset has been replicated. It is
   * guaranteed that upon evaluating the effect, all the changes before this offset are visible in
   * the `EventualJournal`.
   *
   * It is advised not to block semantically in the effect here, because it would slow down the
   * replication process.
   *
   * On subsequent calls to `onReplicatedOffset` for a topic-partition, you might observe offsets
   * smaller than the ones you saw before. This is possible when a topic-partition replication
   * process is restarted from a last committed offset and replays events. The implementations are
   * required to handle this situation gracefully, i.e. ignoring the offsets smaller than the
   * previously seen ones.
   *
   * @param topicPartition
   *   topic partition
   * @param offset
   *   offset, until which (including the changes at the offset itself) all the changes in the topic
   *   partition have been replicated
   */
  def onReplicatedOffset(topicPartition: TopicPartition, offset: Offset): F[Unit]
}

object ReplicatedOffsetNotifier {

  /**
   * [[ReplicatedOffsetNotifier]] implementation which does nothing
   * @tparam F
   *   effect type
   */
  def empty[F[_]: Applicative]: ReplicatedOffsetNotifier[F] = (_: TopicPartition, _: Offset) => Applicative[F].unit
}
