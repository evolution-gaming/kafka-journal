package com.evolutiongaming.kafka.journal.conversions

import cats.ApplicativeError
import cats.implicits._
import com.evolutiongaming.kafka.journal.{ActionHeader, JournalError, ToBytes}
import com.evolutiongaming.skafka.Header

trait ActionHeaderToHeader[F[_]] {

  def apply(actionHeader: ActionHeader): F[Header]
}

object ActionHeaderToHeader {

  implicit def apply[F[_]](implicit
    F: ApplicativeError[F, Throwable],
    actionHeaderToBytes: ToBytes[F, ActionHeader]
  ): ActionHeaderToHeader[F] = {
    actionHeader: ActionHeader => {
      val result = for {
        bytes <- actionHeaderToBytes(actionHeader)
      } yield {
        Header(ActionHeader.key, bytes.toArray)
      }
      result.handleErrorWith { cause =>
        JournalError(s"ActionHeaderToHeader failed for $actionHeader: $cause", cause.some).raiseError[F, Header]
      }
    }
  }
}
