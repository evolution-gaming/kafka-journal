package com.evolutiongaming.kafka.journal

import cats.effect.*
import cats.syntax.all.*
import cats.{Applicative, ~>}
import com.evolutiongaming.catshelper.ClockHelper.*
import com.evolutiongaming.catshelper.{MonadThrowable, RandomId}
import com.evolutiongaming.kafka.journal.conversions.ActionToProducerRecord
import com.evolutiongaming.skafka.Bytes as _

import java.time.Instant

private[journal] trait Produce[F[_]] {

  def append(
    key: Key,
    range: SeqRange,
    payloadAndType: PayloadAndType,
    metadata: HeaderMetadata,
    headers: Headers,
  ): F[PartitionOffset]

  def delete(key: Key, to: DeleteTo): F[PartitionOffset]

  def purge(key: Key): F[PartitionOffset]

  def mark(key: Key, randomId: RandomId): F[PartitionOffset]
}

private[journal] object Produce {

  def empty[F[_]: Applicative](): Produce[F] = const(PartitionOffset.empty.pure[F])

  def const[F[_]](partitionOffset: F[PartitionOffset]): Produce[F] = {
    class Const
    new Const with Produce[F] {
      def append(
        key: Key,
        range: SeqRange,
        payloadAndType: PayloadAndType,
        metadata: HeaderMetadata,
        headers: Headers,
      ): F[PartitionOffset] = {
        partitionOffset
      }

      def delete(key: Key, to: DeleteTo): F[PartitionOffset] = partitionOffset

      def purge(key: Key): F[PartitionOffset] = partitionOffset

      def mark(key: Key, randomId: RandomId): F[PartitionOffset] = partitionOffset
    }
  }

  def apply[F[_]: MonadThrowable: Clock](producer: Journals.Producer[F], origin: Option[Origin])(
    implicit actionToProducerRecord: ActionToProducerRecord[F],
  ): Produce[F] = {
    val produceAction = ProduceAction(producer)
    apply(produceAction, origin)
  }

  def apply[F[_]: MonadThrowable: Clock](
    produceAction: ProduceAction[F],
    origin: Option[Origin],
    version: Version = Version.current,
  ): Produce[F] = {

    def send(action: Action) = {
      produceAction(action).adaptError {
        case e =>
          JournalError(s"failed to produce $action", e)
      }
    }

    class Main
    new Main with Produce[F] {

      def append(
        key: Key,
        range: SeqRange,
        payloadAndType: PayloadAndType,
        metadata: HeaderMetadata,
        headers: Headers,
      ): F[PartitionOffset] = {

        def actionOf(timestamp: Instant) = {
          Action.Append(
            key,
            timestamp,
            ActionHeader.Append(
              range       = range,
              origin      = origin,
              version     = version,
              payloadType = payloadAndType.payloadType,
              metadata    = metadata,
            ),
            payloadAndType.payload,
            headers,
          )
        }

        for {
          timestamp <- Clock[F].instant
          action     = actionOf(timestamp)
          result    <- send(action)
        } yield result
      }

      def delete(key: Key, to: DeleteTo): F[PartitionOffset] = {
        for {
          timestamp <- Clock[F].instant
          action     = Action.Delete(key, timestamp, to, origin, version)
          result    <- send(action)
        } yield result
      }

      def purge(key: Key): F[PartitionOffset] = {
        for {
          timestamp <- Clock[F].instant
          action     = Action.Purge(key, timestamp, origin, version)
          result    <- send(action)
        } yield result
      }

      def mark(key: Key, randomId: RandomId): F[PartitionOffset] = {
        for {
          timestamp <- Clock[F].instant
          id         = randomId.value
          action     = Action.Mark(key, timestamp, id, origin, version)
          result    <- send(action)
        } yield result
      }
    }
  }

  private sealed abstract class MapK

  implicit class ProduceOps[F[_]](val self: Produce[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): Produce[G] = new MapK with Produce[G] {

      def append(
        key: Key,
        range: SeqRange,
        payloadAndType: PayloadAndType,
        metadata: HeaderMetadata,
        headers: Headers,
      ): G[PartitionOffset] = {
        f(self.append(key, range, payloadAndType, metadata, headers))
      }

      def delete(key: Key, to: DeleteTo): G[PartitionOffset] = f(self.delete(key, to))

      def purge(key: Key): G[PartitionOffset] = f(self.purge(key))

      def mark(key: Key, randomId: RandomId): G[PartitionOffset] = f(self.mark(key, randomId))
    }
  }
}
