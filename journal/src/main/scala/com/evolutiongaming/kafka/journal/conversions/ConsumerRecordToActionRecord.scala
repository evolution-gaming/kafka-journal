package com.evolutiongaming.kafka.journal.conversions

import java.time.Instant

import cats.MonadError
import cats.implicits._
import cats.data.OptionT
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

trait ConsumerRecordToActionRecord[F[_]] {

  def apply(consumerRecord: ConsumerRecord[Id, ByteVector]): OptionT[F, ActionRecord[Action]] // TODO change return type
}

object ConsumerRecordToActionRecord {

  implicit def apply[F[_]](implicit
    F: MonadError[F, Throwable],
    consumerRecordToActionHeader: ConsumerRecordToActionHeader[F],
    headerToTuple: HeaderToTuple[F],
  ): ConsumerRecordToActionRecord[F] = {

    consumerRecord: ConsumerRecord[Id, ByteVector] => {

      def action(key: Key, timestamp: Instant, header: ActionHeader) = {

        def append(header: ActionHeader.Append) = {
          consumerRecord.value.traverse { value =>
            val headers = consumerRecord.headers
              .filter { _.key != ActionHeader.key }
              .traverse(headerToTuple.apply)

            for {
              headers <- headers
            } yield {
              val payload = value.value
              Action.append(key, timestamp, header, payload, headers.toMap)
            }
          }
        }

        header match {
          case header: ActionHeader.Append => OptionT(append(header))
          case header: ActionHeader.Delete => OptionT.pure[F](Action.delete(key, timestamp, header))
          case header: ActionHeader.Mark   => OptionT.pure[F](Action.mark(key, timestamp, header))
        }
      }

      val opt = for {
        id               <- consumerRecord.key
        timestampAndType <- consumerRecord.timestampAndType
        header           <- consumerRecordToActionHeader(consumerRecord)
      } yield for {
        header    <- OptionT.liftF(header)
        key        = Key(id = id.value, topic = consumerRecord.topic)
        timestamp  = timestampAndType.timestamp
        action    <- action(key, timestamp, header)
      } yield {
        val partitionOffset = PartitionOffset(consumerRecord)
        ActionRecord(action, partitionOffset)
      }

      val result = OptionT.fromOption[F](opt).flatten.value.handleErrorWith { cause =>
        JournalError(s"consumerRecordToActionRecord failed for $consumerRecord: $cause", cause.some).raiseError[F, Option[ActionRecord[Action]]]
      }
      OptionT(result)
    }
  }
}
