package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel, NonEmptySet => Nes}
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.kafka.journal.HeadCache.{Consumer, ConsRecordToKafkaRecord, KafkaRecord}
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.sstream.Stream

import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import cats.effect.Temporal

object HeadCacheConsumption {

  def apply[F[_] : BracketThrowable : Temporal](
    topic: Topic,
    pointers: F[Map[Partition, Offset]],
    consumer: Resource[F, Consumer[F]],
    log: Log[F])(implicit
    consRecordToKafkaRecord: ConsRecordToKafkaRecord[F]
  ): Stream[F, List[(Partition, Nel[KafkaRecord])]] = {

    def kafkaRecords(records: ConsRecords): F[List[(Partition, Nel[KafkaRecord])]] = {
      records
        .values
        .toList
        .traverseFilter { case (partition, records) =>
          records
            .toList
            .traverseFilter { record => consRecordToKafkaRecord(record).value }
            .map { records => records.toNel.map { records => (partition.partition, records) } }
        }
    }

    def poll(consumer: Consumer[F]) = {
      for {
        records <- consumer.poll
        records <- kafkaRecords(records)
      } yield records
    }

    def partitions(consumer: Consumer[F]): F[Nes[Partition]] = {

      val partitions = for {
        partitions <- consumer.partitions(topic)
        partitions <- partitions.toList.toNel.fold {
          NoPartitionsError.raiseError[F, Nel[Partition]]
        } { partitions =>
          partitions.pure[F]
        }
      } yield {
        partitions.toNes
      }

      val retry = for {
        random <- Random.State.fromClock[F]()
      } yield {
        val strategy = Strategy
          .exponential(10.millis)
          .jitter(random)
          .limit(1.minute)
        val onError = OnError.fromLog(log.prefixed(s"consumer.partitions"))
        Retry(strategy, onError)
      }

      for {
        retry      <- retry
        partitions <- retry(partitions)
      } yield partitions
    }

    def offsets(partitions: Nes[Partition], pointers: Map[Partition, Offset]) = {
      partitions.toNel.traverse { partition =>
        pointers
          .get(partition)
          .fold(Offset.min.pure[F]) { _.inc[F] }
          .map { offset => (partition, offset) }
      }
    }

    val retry = for {
      random <- Random.State.fromClock[F]()
    } yield {
      val strategy = Strategy
        .exponential(10.millis)
        .jitter(random)
        .cap(1.second)
        .resetAfter(1.minute)
      val onError = OnError.fromLog(log.prefixed("consuming"))
      Retry(strategy, onError)
    }

    def seek(consumer: Consumer[F]) = {
      for {
        partitions <- partitions(consumer)
        _          <- consumer.assign(topic, partitions)
        pointers   <- pointers
        offsets    <- offsets(partitions, pointers)
        _          <- consumer.seek(topic, offsets.toNem)
      } yield {}
    }

    for {
      retry    <- Stream.lift(retry)
      _        <- Stream.around(retry.toFunctionK)
      consumer <- Stream.fromResource(consumer)
      _        <- Stream.lift(seek(consumer))
      records  <- Stream.repeat(poll(consumer)) if records.nonEmpty
    } yield records
  }


  case object NoPartitionsError extends RuntimeException("No partitions") with NoStackTrace
}
