package com.evolutiongaming.kafka.journal.conversions

import cats.ApplicativeError
import cats.implicits._
import com.evolutiongaming.kafka.journal.{JournalError, ToBytes}
import com.evolutiongaming.skafka.Header

trait TupleToHeader[F[_]] {

  def apply(key: String, value: String): F[Header]
}

object TupleToHeader {

  implicit def apply[F[_]](implicit
    F: ApplicativeError[F, Throwable],
    stringToBytes: ToBytes[F, String],
  ): TupleToHeader[F] = {
    (key, value) =>
      val result = for {
        value <- stringToBytes(value)
      } yield {
        Header(key, value.toArray)
      }
      result.handleErrorWith { cause =>
        JournalError(s"TupleToHeader failed for $key:$value: $cause", cause.some).raiseError[F, Header]
      }
  }
}
