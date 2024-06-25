package com.evolutiongaming.kafka.journal.conversions

import cats.syntax.all._
import com.evolutiongaming.catshelper.ApplicativeThrowable
import com.evolutiongaming.kafka.journal.{ActionHeader, JournalError, ToBytes}
import com.evolutiongaming.skafka.Header

trait ActionHeaderToHeader[F[_]] {

  def apply(actionHeader: ActionHeader): F[Header]
}

object ActionHeaderToHeader {

  implicit def apply[F[_]: ApplicativeThrowable](implicit
    actionHeaderToBytes: ToBytes[F, ActionHeader],
  ): ActionHeaderToHeader[F] = { (actionHeader: ActionHeader) =>
    val result = for {
      bytes <- actionHeaderToBytes(actionHeader)
    } yield Header(ActionHeader.key, bytes.toArray)
    result.adaptErr {
      case e =>
        JournalError(s"ActionHeaderToHeader failed for $actionHeader: $e", e)
    }
  }
}
