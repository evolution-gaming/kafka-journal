package com.evolutiongaming.kafka.journal.conversions

import cats.data.OptionT
import cats.syntax.all._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.skafka.Header
import scodec.bits.ByteVector

trait ConsRecordToActionHeader[F[_]] {

  def apply(consRecord: ConsRecord): OptionT[F, ActionHeader]
}

object ConsRecordToActionHeader {

  implicit def apply[F[_] : MonadThrowable](implicit
    fromBytes: FromBytes[F, Option[ActionHeader]]
  ): ConsRecordToActionHeader[F] = {

    consRecord: ConsRecord => {
      def header = consRecord
        .headers
        .find { _.key === ActionHeader.key }

      def actionHeader(header: Header) = {
        val byteVector = ByteVector.view(header.value)
        fromBytes(byteVector).adaptError { case e =>
          JournalError(s"ConsRecordToActionHeader failed for $consRecord: $e", e)
        }
      }

      for {
        header       <- header.toOptionT[F]
        actionHeader <- actionHeader(header).toOptionT
      } yield actionHeader

    }
  }
}
