package com.evolution.kafka.journal.conversions

import cats.data.OptionT
import cats.syntax.all.*
import com.evolution.kafka.journal.*
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.skafka.consumer.ConsumerRecord

/**
 * Gets [[HeadAction]] from a Kafka record, for [[HeadCache]].
 *
 * Uses [[HeadAction.fromJson]], and [[ConsRecordToActionHeader]] for headers that
 * [[HeadAction.fromJson]] does not decode.
 */
private[journal] trait ConsRecordToHeadAction[F[_]] {

  /**
   * @return
   *   [[HeadAction]] or `F[None]` if [[ActionHeader.key]] is not found. May raise a
   *   [[JournalError]] if the header is found, but could not be parsed.
   */
  def apply[A](record: ConsumerRecord[String, A]): OptionT[F, HeadAction]
}

private[journal] object ConsRecordToHeadAction {

  /**
   * Uses only [[ConsRecordToActionHeader]], i.e. decodes the whole header.
   */
  def full[F[_]: MonadThrowable](consRecordToActionHeader: ConsRecordToActionHeader[F]): ConsRecordToHeadAction[F] = {
    new ConsRecordToHeadAction[F] {

      def apply[A](record: ConsumerRecord[String, A]): OptionT[F, HeadAction] = {
        consRecordToActionHeader(record).map { HeadAction(_) }
      }
    }
  }

  def apply[F[_]: MonadThrowable](fallback: ConsRecordToActionHeader[F]): ConsRecordToHeadAction[F] = {
    new ConsRecordToHeadAction[F] {

      def apply[A](record: ConsumerRecord[String, A]): OptionT[F, HeadAction] = {
        record
          .headers
          .find { _.key === ActionHeader.key }
          .fold {
            OptionT.none[F, HeadAction]
          } { header =>
            HeadAction.fromJson(header.value) match {
              case Some(action) => OptionT.fromOption[F](action)
              case None => fallback(record).map { HeadAction(_) }
            }
          }
      }
    }
  }
}
