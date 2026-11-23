package com.evolution.kafka.journal

import cats.Applicative
import cats.effect.Resource
import cats.syntax.all.*
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

/**
 * Request for [[HeadInfo]] of a journal, sent to the pod owning the journal's partition in
 * [[ShardedHeadCache]].
 *
 * @param topic
 *   Topic where journal is stored.
 * @param partition
 *   Partition where journal is stored.
 * @param id
 *   Journal identifier.
 * @param offset
 *   Offset up to which the owner must have consumed the partition before answering, i.e. the offset
 *   of the marker written by the recovery.
 */
final case class HeadCacheRequest(
  topic: Topic,
  partition: Partition,
  id: String,
  offset: Offset,
)

/**
 * Communication between pods sharing a [[ShardedHeadCache]].
 *
 * kafka-journal implements the head cache consumer, but not the communication between owners of the
 * shard. Applications must plug their own implementations of the communication, which may be
 * specific to the environment they are deployed in.
 *
 * The answer for a journal at an offset does not depend on which pod gives it, so the
 * implementation may deliver a request to several pods serving the same partition and use the first
 * answer.
 */
trait HeadCacheTransport[F[_]] {

  /**
   * Start serving requests for a partition.
   *
   * This is the server part of the head cache. Started when the partition is assigned to this pod.
   * Released when the partition is revoked, which should stop serving requests to the partition.
   *
   * @param handle
   *   Answers a request. `None` means this pod can't answer, in which case the implementation must
   *   not reply, so that it does not hide an answer from another pod serving the same partition.
   */
  def serve(
    topic: Topic,
    partition: Partition,
    handle: HeadCacheRequest => F[Option[HeadInfo]],
  ): Resource[F, Unit]

  /**
   * Ask the pod serving the request's partition.
   *
   * This is the client part of the head cache. The implementation must send requests that will be
   * received by the matching server implemented in [[serve]].
   *
   * @return
   *   `None` if no pod is available or if no answer was received in time.
   */
  def request(request: HeadCacheRequest): F[Option[HeadInfo]]
}

object HeadCacheTransport {

  /**
   * Serves nothing and never gets an answer, i.e. every pod uses only the partitions it owns.
   */
  def empty[F[_]: Applicative]: HeadCacheTransport[F] = new Empty

  private final class Empty[F[_]: Applicative] extends HeadCacheTransport[F] {
    def serve(
      topic: Topic,
      partition: Partition,
      handle: HeadCacheRequest => F[Option[HeadInfo]],
    ): Resource[F, Unit] = Resource.unit[F]

    def request(request: HeadCacheRequest): F[Option[HeadInfo]] = none[HeadInfo].pure[F]
  }
}
