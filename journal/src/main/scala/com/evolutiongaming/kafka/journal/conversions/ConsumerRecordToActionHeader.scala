package com.evolutiongaming.kafka.journal.conversions

import cats.ApplicativeError
import cats.implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

trait ConsumerRecordToActionHeader[F[_]] {

  def apply(consumerRecord: ConsumerRecord[String, ByteVector]): Option[F[ActionHeader]]
}

object ConsumerRecordToActionHeader {

  implicit def apply[F[_]](implicit
    F: ApplicativeError[F, Throwable],
    fromBytes: FromBytes[F, ActionHeader]
  ): ConsumerRecordToActionHeader[F] = {

    consumerRecord: ConsumerRecord[String, ByteVector] => {
      for {
        header <- consumerRecord.headers.find { _.key == ActionHeader.key }
      } yield {
        val byteVector = ByteVector.view(header.value)
        val actionHeader = fromBytes(byteVector)
        actionHeader.handleErrorWith { cause =>
          JournalError(s"ConsumerRecordToActionHeader failed for $consumerRecord: $cause", cause.some).raiseError[F, ActionHeader]
        }
      }
    }
  }
}
