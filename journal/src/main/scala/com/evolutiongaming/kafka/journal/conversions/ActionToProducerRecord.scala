package com.evolutiongaming.kafka.journal.conversions

import cats.MonadError
import cats.implicits._
import com.evolutiongaming.kafka.journal.{Action, JournalError}
import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.producer.ProducerRecord
import scodec.bits.ByteVector

trait ActionToProducerRecord[F[_]] {

  def apply(action: Action): F[ProducerRecord[String, ByteVector]]
}

object ActionToProducerRecord {

  implicit def apply[F[_]](implicit
    F: MonadError[F, Throwable],
    actionHeaderToHeader: ActionHeaderToHeader[F],
    tupleToHeader: TupleToHeader[F]
  ): ActionToProducerRecord[F] = {

    action: Action => {
      val key = action.key
      val result = for {
        header  <- actionHeaderToHeader(action.header)
        headers <- action match {
          case a: Action.Append => a.headers.toList.traverse { case (k, v) => tupleToHeader(k, v) }
          case _: Action.Delete => List.empty[Header].pure[F]
          case _: Action.Mark   => List.empty[Header].pure[F]
        }
      } yield {
        val payload = action match {
          case a: Action.Append => a.payload.some
          case _: Action.Delete => none
          case _: Action.Mark   => none
        }

        ProducerRecord(
          topic = key.topic,
          value = payload,
          key = key.id.some,
          timestamp = action.timestamp.some,
          headers = header :: headers)
      }
      result.handleErrorWith { cause =>
        JournalError(s"ActionToProducerRecord failed for $action: $cause", cause.some).raiseError[F, ProducerRecord[String, ByteVector]]
      }
    }
  }
}
