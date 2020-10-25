package com.evolutiongaming.kafka.journal.conversions

import java.time.Instant

import cats.data.OptionT
import cats.syntax.all._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.util.CatsHelper._

trait ConsRecordToActionRecord[F[_]] {

  def apply(consRecord: ConsRecord): OptionT[F, ActionRecord[Action]]
}


object ConsRecordToActionRecord {

  implicit def apply[F[_] : MonadThrowable](implicit
    consRecordToActionHeader: ConsRecordToActionHeader[F],
    headerToTuple: HeaderToTuple[F],
  ): ConsRecordToActionRecord[F] = {

    consRecord: ConsRecord => {

      def action(key: Key, timestamp: Instant, header: ActionHeader) = {

        def append(header: ActionHeader.Append) = {
          consRecord
            .value
            .traverse { value =>
              val headers = consRecord.headers
                .filter { _.key =!= ActionHeader.key }
                .traverse { header => headerToTuple(header) }

              for {
                headers <- headers
              } yield {
                val payload = value.value
                Action.append(key, timestamp, header, payload, headers.toMap)
              }
            }
        }

        header match {
          case header: ActionHeader.Append => append(header).toOptionT
          case header: ActionHeader.Mark   => Action.mark(key, timestamp, header).pure[OptionT[F, *]]
          case header: ActionHeader.Delete => Action.delete(key, timestamp, header).pure[OptionT[F, *]]
          case header: ActionHeader.Purge  => Action.purge(key, timestamp, header).pure[OptionT[F, *]]
        }
      }

      val result = for {
        id               <- consRecord.key.toOptionT[F]
        timestampAndType <- consRecord.timestampAndType.toOptionT[F]
        header           <- consRecordToActionHeader(consRecord)
        key               = Key(id = id.value, topic = consRecord.topic)
        timestamp         = timestampAndType.timestamp
        action           <- action(key, timestamp, header)
      } yield {
        val partitionOffset = PartitionOffset(consRecord)
        ActionRecord(action, partitionOffset)
      }

      result
        .value
        .adaptError { case e =>
          JournalError(s"ConsRecordToActionRecord failed for $consRecord: $e", e)
        }
        .toOptionT
    }
  }
}
