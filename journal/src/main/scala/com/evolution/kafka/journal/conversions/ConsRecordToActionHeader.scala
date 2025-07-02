package com.evolution.kafka.journal.conversions

import cats.data.OptionT
import cats.syntax.all.*
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.util.CatsHelper.*
import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

/**
 * Parses Kafka Journal specific header from a generic Kafka record.
 *
 * Example:
 * {{{
 * def pollVersions[F[_]: Monad](
 *   kafkaConsumer: KafkaConsumer[F, String, _],
 *   consRecordToActionHeader: ConsRecordToActionHeader[F]
 * ): F[List[Version]] =
 *   for {
 *     recordsByPartition <- kafkaConsumer.poll(10.seconds)
 *     flattenedRecords = recordsByPartition.values.values.toList.flatMap(_.toList)
 *     actions <- flattenedRecords.traverseFilter { record => consRecordToActionHeader(record).value }
 *   } yield actions.flatMap(_.version)
 * }}}
 */
trait ConsRecordToActionHeader[F[_]] {

  /**
   * Convert generic Kafka record to a Kafka Journal action.
   *
   * @param record
   *   The record received from Kafka.
   * @return
   *   [[ActionHeader]] or `F[None]` if [[ActionHeader.key]] is not found. May raise a
   *   [[JournalError]] if the header is found, but could not be parsed.
   */
  def apply[A](record: ConsumerRecord[String, A]): OptionT[F, ActionHeader]
}

object ConsRecordToActionHeader {

  implicit def apply[F[_]: MonadThrowable](
    implicit
    fromBytes: FromBytes[F, Option[ActionHeader]],
  ): ConsRecordToActionHeader[F] =
    new ConsRecordToActionHeader[F] {

      def apply[A](record: ConsumerRecord[String, A]) = {
        def header = record
          .headers
          .find { _.key === ActionHeader.key }

        def actionHeader(header: Header) = {
          val byteVector = ByteVector.view(header.value)
          fromBytes(byteVector).adaptError {
            case e =>
              JournalError(s"ConsRecordToActionHeader failed for $record: $e", e)
          }
        }

        for {
          header <- header.toOptionT[F]
          actionHeader <- actionHeader(header).toOptionT
        } yield actionHeader
      }
    }
}
