package com.evolutiongaming.kafka.journal

import java.time.Instant

import cats.effect._
import cats.implicits._
import cats.~>
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.conversions.ActionToProducerRecord
import com.evolutiongaming.skafka.{Bytes => _}

trait Produce[F[_]] {

  def append(
    key: Key,
    range: SeqRange,
    payloadAndType: PayloadAndType,
    metadata: HeaderMetadata,
    headers: Headers
  ): F[PartitionOffset]

  def delete(key: Key, to: DeleteTo): F[PartitionOffset]

  def purge(key: Key): F[PartitionOffset]

  def mark(key: Key, randomId: RandomId): F[PartitionOffset]
}

object Produce {

  def apply[F[_] : MonadThrowable : Clock](
    producer: Journals.Producer[F],
    origin: Option[Origin])(implicit
    actionToProducerRecord: ActionToProducerRecord[F]
  ): Produce[F] = {
    val produceAction = ProduceAction(producer)
    apply(produceAction, origin)
  }

  def apply[F[_] : MonadThrowable : Clock](
    produceAction: ProduceAction[F],
    origin: Option[Origin]
  ): Produce[F] = {

    def send(action: Action) = {
      produceAction(action).handleErrorWith { cause =>
        val error = JournalError(s"failed to produce $action", cause)
        error.raiseError[F, PartitionOffset]
      }
    }

    new Produce[F] {

      def append(
        key: Key,
        range: SeqRange,
        payloadAndType: PayloadAndType,
        metadata: HeaderMetadata,
        headers: Headers
      ) = {

        // TODO expiry: use metadata.payload
        def actionOf(timestamp: Instant) = {
          Action.Append(
            key,
            timestamp,
            ActionHeader.Append(
              range = range,
              origin = origin,
              payloadType = payloadAndType.payloadType,
              metadata = metadata),
            payloadAndType.payload,
            headers)
        }

        for {
          timestamp <- Clock[F].instant
          action     = actionOf(timestamp)
          result    <- send(action)
        } yield result
      }

      def delete(key: Key, to: DeleteTo) = {
        for {
          timestamp <- Clock[F].instant
          action     = Action.Delete(key, timestamp, to, origin)
          result    <- send(action)
        } yield result
      }

      def purge(key: Key) = {
        for {
          timestamp <- Clock[F].instant
          action     = Action.Purge(key, timestamp, origin)
          result    <- send(action)
        } yield result
      }

      def mark(key: Key, randomId: RandomId) = {
        for {
          timestamp <- Clock[F].instant
          id         = randomId.value
          action     = Action.Mark(key, timestamp, id, origin)
          result    <- send(action)
        } yield result
      }
    }
  }


  implicit class ProduceOps[F[_]](val self: Produce[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): Produce[G] = new Produce[G] {

      def append(
        key: Key,
        range: SeqRange,
        payloadAndType: PayloadAndType,
        metadata: HeaderMetadata,
        headers: Headers
      ) = {
        f(self.append(key, range, payloadAndType, metadata, headers))
      }

      def delete(key: Key, to: DeleteTo) = f(self.delete(key, to))

      def purge(key: Key) = f(self.purge(key))

      def mark(key: Key, randomId: RandomId) = f(self.mark(key, randomId))
    }
  }
}
