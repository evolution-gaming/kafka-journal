package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel, NonEmptySet => Nes}
import cats.effect.{Resource, Timer}
import cats.syntax.all._
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.kafka.journal.TopicCache.Consumer
import com.evolutiongaming.kafka.journal.util.StreamHelper._
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.{OnError, Retry, Strategy}
import com.evolutiongaming.retry.Retry.implicits._
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.sstream.Stream

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

object HeadCacheConsumption {

  def apply[F[_] : BracketThrowable : Timer](
    topic: Topic,
    pointers: F[Map[Partition, Offset]],
    consumer: Resource[F, TopicCache.Consumer[F]],
    log: Log[F]
  ): Stream[F, ConsumerRecords[String, Unit]] = {

    def partitions(consumer: Consumer[F], random: Random.State): F[Nes[Partition]] = {
      val partitions = for {
        partitions <- consumer.partitions(topic)
        partitions <- partitions
          .toList
          .toNel
          .fold { NoPartitionsError.raiseError[F, Nel[Partition]] } { _.pure[F] }
      } yield {
        partitions.toNes
      }

      val strategy = Strategy
        .exponential(10.millis)
        .jitter(random)
        .limit(1.minute)
      val onError = OnError.fromLog(log.prefixed(s"consumer.partitions"))
      partitions.retry(strategy, onError)
    }

    def seek(consumer: Consumer[F], random: Random.State) = {
      for {
        partitions <- partitions(consumer, random)
        _          <- consumer.assign(topic, partitions)
        pointers   <- pointers
        offsets     = partitions
          .toNel
          .map { partition =>
            val offset = pointers.getOrElse(partition, Offset.min)
            (partition, offset)
          }
          .toNem
        result     <- consumer.seek(topic, offsets)
      } yield result
    }

    for {
      random   <- Random.State.fromClock[F]().toStream
      retry     = {
        val strategy = Strategy
          .exponential(10.millis)
          .jitter(random)
          .cap(1.second)
          .resetAfter(1.minute)
        val onError = OnError.fromLog(log.prefixed("consuming"))
        Retry(strategy, onError)
      }
      _        <- Stream.around(retry.toFunctionK)
      consumer <- consumer.toStream
      _        <- seek(consumer, random).toStream
      records  <- Stream.repeat(consumer.poll) if records.values.nonEmpty
    } yield records
  }


  case object NoPartitionsError extends RuntimeException("No partitions") with NoStackTrace
}
