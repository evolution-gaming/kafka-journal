package com.evolutiongaming.kafka.journal.conversions

import cats.syntax.all.*
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.{Action, JournalError}
import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.producer.ProducerRecord
import scodec.bits.ByteVector

private[journal] trait ActionToProducerRecord[F[_]] {

  def apply(action: Action): F[ProducerRecord[String, ByteVector]]
}

private[journal] object ActionToProducerRecord {

  implicit def apply[F[_]: MonadThrowable](
    implicit actionHeaderToHeader: ActionHeaderToHeader[F],
    tupleToHeader: TupleToHeader[F],
  ): ActionToProducerRecord[F] = { (action: Action) =>
    {
      val key = action.key
      val result = for {
        header <- actionHeaderToHeader(action.header)
        headers <- action match {
          case a: Action.Append => a.headers.toList.traverse { case (k, v) => tupleToHeader(k, v) }
          case _: Action.Mark   => List.empty[Header].pure[F]
          case _: Action.Delete => List.empty[Header].pure[F]
          case _: Action.Purge  => List.empty[Header].pure[F]
        }
      } yield {
        val payload = action match {
          case a: Action.Append => a.payload.some
          case _: Action.Mark   => none
          case _: Action.Delete => none
          case _: Action.Purge  => none
        }

        ProducerRecord(
          topic     = key.topic,
          value     = payload,
          key       = key.id.some,
          timestamp = action.timestamp.some,
          headers   = header :: headers,
        )
      }
      result.handleErrorWith { cause =>
        JournalError(s"ActionToProducerRecord failed for $action: $cause", cause)
          .raiseError[F, ProducerRecord[String, ByteVector]]
      }
    }
  }
}
