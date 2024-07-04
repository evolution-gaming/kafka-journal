package com.evolutiongaming.kafka.journal.conversions

import cats.syntax.all._
import com.evolutiongaming.catshelper.ApplicativeThrowable
import com.evolutiongaming.kafka.journal.{JournalError, ToBytes}
import com.evolutiongaming.skafka.Header

trait TupleToHeader[F[_]] {

  def apply(key: String, value: String): F[Header]
}

object TupleToHeader {

  implicit def apply[F[_]: ApplicativeThrowable](implicit stringToBytes: ToBytes[F, String]): TupleToHeader[F] = { (key, value) =>
    val result = for {
      value <- stringToBytes(value)
    } yield {
      Header(key, value.toArray)
    }
    result.adaptErr {
      case e =>
        JournalError(s"TupleToHeader failed for $key:$value: $e", e)
    }
  }
}
