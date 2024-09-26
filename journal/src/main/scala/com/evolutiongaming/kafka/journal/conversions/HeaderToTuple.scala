package com.evolutiongaming.kafka.journal.conversions

import cats.syntax.all.*
import com.evolutiongaming.catshelper.ApplicativeThrowable
import com.evolutiongaming.kafka.journal.{FromBytes, JournalError}
import com.evolutiongaming.skafka.Header
import scodec.bits.ByteVector

private[journal] trait HeaderToTuple[F[_]] {

  def apply(header: Header): F[(String, String)]
}

private[journal] object HeaderToTuple {

  implicit def apply[F[_]: ApplicativeThrowable](implicit stringFromBytes: FromBytes[F, String]): HeaderToTuple[F] = {
    (header: Header) =>
      {
        val bytes = ByteVector.view(header.value)
        stringFromBytes(bytes)
          .map { value => (header.key, value) }
          .adaptErr { case e => JournalError(s"HeaderToTuple failed for $header: $e", e) }
      }
  }
}
