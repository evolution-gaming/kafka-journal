package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.effect.{ContextShift, Sync, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.HeadCache.{Consumer, ConsumerRecordToKafkaRecord, KafkaRecord, NoPartitionsError}
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import scodec.bits.ByteVector

import scala.concurrent.duration._

object HeadCacheConsumer {

  def apply[F[_] : Sync : Timer : ContextShift](
    topic: Topic,
    from: Map[Partition, Offset],
    pollTimeout: FiniteDuration,
    consumer: Consumer[F], // TODO resource
    cancel: F[Boolean],
    log: Log[F])(
    onRecords: Map[Partition, Nel[KafkaRecord]] => F[Unit])(implicit
    consumerRecordToKafkaRecord: ConsumerRecordToKafkaRecord[F]
  ): F[Unit] = {

    def kafkaRecords(records: ConsumerRecords[String, ByteVector]): F[List[(Partition, Nel[KafkaRecord])]] = {
      records
        .values
        .toList
        .traverseFilter { case (partition, records) =>
          records
            .toList
            .traverseFilter { record => consumerRecordToKafkaRecord(record).sequence }
            .map { records => Nel.fromList(records).map { records => (partition.partition, records) } }
        }
    }

    val poll = for {
      cancel <- cancel
      result <- {
        if (cancel) ().some.pure[F]
        else for {
          records0 <- consumer.poll(pollTimeout)
          _        <- if (records0.values.isEmpty) ContextShift[F].shift else ().pure[F] // TODO remove from here
          records  <- kafkaRecords(records0)
          _        <- if (records.isEmpty) ().pure[F] else onRecords(records.toMap)
        } yield none[Unit]
      }
    } yield result

    val partitions: F[Nel[Partition]] = {

      val partitions = for {
        partitions <- consumer.partitions(topic)
        partitions <- Nel.fromList(partitions.toList) match {
          case Some(a) => a.pure[F]
          case None    => NoPartitionsError.raiseError[F, Nel[Partition]]
        }
      } yield partitions

      for {
        random     <- Random.State.fromClock[F]()
        strategy    = Strategy.fullJitter(3.millis, random).cap(300.millis)
        onError     = OnError.fromLog(log.prefixed(s"consumer.partitions($topic)"))
        retry       = Retry(strategy, onError)
        partitions <- retry(partitions)
      } yield partitions
    }

    for {
      partitions <- partitions
      _          <- consumer.assign(topic, partitions)
      offsets     = for {
        partition <- partitions
      } yield {
        val offset = from.get(partition).fold(Offset.Min)(_ + 1L)
        (partition, offset)
      }
      _          <- consumer.seek(topic, offsets.toList.toMap)
      _          <- poll.untilDefinedM
    } yield {}
  }
}
