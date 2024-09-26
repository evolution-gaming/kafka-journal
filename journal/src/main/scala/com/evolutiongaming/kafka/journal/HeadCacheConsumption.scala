package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList as Nel, NonEmptySet as Nes}
import cats.effect.Resource
import cats.syntax.all.*
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.journal.TopicCache.Consumer
import com.evolutiongaming.kafka.journal.util.StreamHelper.*
import com.evolutiongaming.random.Random
import com.evolutiongaming.retry.Retry.implicits.*
import com.evolutiongaming.retry.{OnError, Retry, Sleep, Strategy}
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.sstream.Stream

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

private[journal] object HeadCacheConsumption {

  /** Streams records from Kafka topic with error handling and retries.
    *
    * If a consumer fails then it will be recreated, so the consumption is
    * continued. The retry procedure will happen with some random jitter to
    * ensure different nodes do not intefere with each other.
    *
    * Note, that `pointer` parameter is also wrapped in `F[_]`, i.e. the latest
    * partition offsets will be used on each consumer failure, so head cache
    * does not have to read records, which were already seen.
    *
    * @param topic
    *   Kafka topic where journal events are stored.
    * @param pointers
    *   Partition offsets to start the reading from. These offsets, usually,
    *   come from the cache itself, which gets prepopulated by the information
    *   received from Cassandra. If partition is not included into a `Map` then
    *   [[Offset#min]] will be used, instead.
    * @param consumer
    *   Kafka consumer factory. The consumer might be recreated in case of the
    *   error.
    * @param log
    *   Log to write the consumer failures to.
    * @return
    *   Records from Kafka topic for all the partitions in `topic`. The method
    *   does not raise erros, but tries to restart consumer on failure until it
    *   succeeds.
    */
  def apply[F[_]: BracketThrowable: Sleep](
    topic: Topic,
    pointers: F[Map[Partition, Offset]],
    consumer: Resource[F, TopicCache.Consumer[F]],
    log: Log[F],
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
        offsets = partitions
          .toNel
          .map { partition =>
            val offset = pointers.getOrElse(partition, Offset.min)
            (partition, offset)
          }
          .toNem
        result <- consumer.seek(topic, offsets)
      } yield result
    }

    for {
      random <- Random.State.fromClock[F]().toStream
      retry = {
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

  /** Consumer did not return any partitions.
    *
    * This consumer does not use consumer groups, i.e. all partitions should
    * have been returned, so the likely reason could be that the topic is not
    * properly initialized yet.
    */
  case object NoPartitionsError extends RuntimeException("No partitions") with NoStackTrace
}
