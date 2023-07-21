package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel, NonEmptySet => Nes}
import cats.effect.kernel.Concurrent
import cats.effect.{Clock, Deferred, Resource}
import cats.syntax.all._
import cats.effect.syntax.resource._
import cats.~>
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.journal.conversions.ConsRecordToActionRecord
import com.evolutiongaming.kafka.journal.util.ResourcePool
import com.evolutiongaming.kafka.journal.util.StreamHelper._
import com.evolutiongaming.skafka.{Offset, Partition, Topic, TopicPartition}
import com.evolutiongaming.sstream.Stream

import scala.concurrent.duration.FiniteDuration

trait ConsumeActionRecords[F[_]] {

  def apply(key: Key, partition: Partition, from: Offset): Stream[F, ActionRecord[Action]]
}

object ConsumeActionRecords {

  @deprecated("use `of` instead", "2023-07-26")
  def apply[F[_] : BracketThrowable](
    consumer: Resource[F, Journals.Consumer[F]],
    log: Log[F])(implicit
    consRecordToActionRecord: ConsRecordToActionRecord[F]
  ): ConsumeActionRecords[F] = apply1(_ => consumer, log)

  def of[F[_] : Concurrent : Clock](
    consumerPool: ResourcePool[F, Journals.Consumer[F]],
    poolMetrics: ConsumerPoolMetrics[F],
    log: Log[F],
  )(implicit
    consRecordToActionRecord: ConsRecordToActionRecord[F],
  ): ConsumeActionRecords[F] = {

    def consumerWithAcquisitionMetrics(topic: Topic): Resource[F, Journals.Consumer[F]] =
      for {
        startedAcquiringAt <- Clock[F].monotonic.toResource
        deferred <- Deferred[F, FiniteDuration].toResource
        consumer <- consumerPool.borrow.onFinalize {
          for {
            start <- deferred.get
            end <- Clock[F].monotonic
            _ <- poolMetrics.useTime(topic, end - start)
          } yield ()
        }
        acquiredAt <- Clock[F].monotonic.toResource
        _ <- deferred.complete(acquiredAt).toResource
        _ <- poolMetrics.acquireTime(topic, acquiredAt - startedAcquiringAt).toResource
      } yield consumer

    apply1(consumerWithAcquisitionMetrics, log)
  }

  private[journal] def apply1[F[_] : BracketThrowable](
    makeConsumer: Topic => Resource[F, Journals.Consumer[F]],
    log: Log[F])(implicit
    consRecordToActionRecord: ConsRecordToActionRecord[F]
  ): ConsumeActionRecords[F] = {
    (key: Key, partition: Partition, from: Offset) => {

      val topicPartition = TopicPartition(topic = key.topic, partition = partition)

      def seek(consumer: Journals.Consumer[F]) = {
        for {
          _ <- consumer.assign(Nes.of(topicPartition))
          _ <- consumer.seek(topicPartition, from)
          _ <- log.debug(s"$key consuming from $partition:$from")
        } yield {}
      }

      def filter(records: List[Nel[ConsRecord]]) = {
        for {
          records <- records
          record  <- records.toList if record.key.exists { _.value === key.id }
        } yield record
      }

      def poll(consumer: Journals.Consumer[F]) = {
        for {
          records0 <- consumer.poll
          records   = filter(records0.values.values.toList)
          actions  <- records.traverseFilter { a => consRecordToActionRecord(a).value }
        } yield actions
      }

      for {
        consumer <- makeConsumer(key.topic).toStream
        _        <- seek(consumer).toStream
        records  <- Stream.repeat(poll(consumer))
        record   <- records.toStream1[F]
      } yield record
    }
  }

  implicit class ConsumeActionRecordsOps[F[_]](val self: ConsumeActionRecords[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): ConsumeActionRecords[G] = {
      (key: Key, partition: Partition, from1: Offset) => {
        self(key, partition, from1).mapK[G](fg, gf)
      }
    }
  }
}