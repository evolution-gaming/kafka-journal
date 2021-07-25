package com.evolutiongaming.kafka.journal.conversions

import cats.data.OptionT
import cats.syntax.all._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

trait ConsRecordToActionHeader[F[_]] {

  def apply[A](record: ConsumerRecord[String, A]): OptionT[F, ActionHeader]
}

object ConsRecordToActionHeader {

  implicit def apply[F[_] : MonadThrowable](implicit
    fromBytes: FromBytes[F, Option[ActionHeader]]
  ): ConsRecordToActionHeader[F] = new ConsRecordToActionHeader[F] {

    def apply[A](record: ConsumerRecord[String, A]) = {
      def header = record
        .headers
        .find { _.key === ActionHeader.key }

      def actionHeader(header: Header) = {
        val byteVector = ByteVector.view(header.value)
        fromBytes(byteVector).adaptError { case e =>
          JournalError(s"ConsRecordToActionHeader failed for $record: $e", e)
        }
      }

      for {
        header       <- header.toOptionT[F]
        actionHeader <- actionHeader(header).toOptionT
      } yield actionHeader
    }
  }
}
