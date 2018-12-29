package com.evolutiongaming.kafka.journal

import cats.effect.IO
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.kafka.journal.util.IOHelper._
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.{Offset, Partition}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object HeadCacheAsync {

  def apply(
    consumerConfig: ConsumerConfig,
    eventualJournal: EventualJournal[Async],
    blocking: ExecutionContext)(implicit
    ec: ExecutionContextExecutor): HeadCache[Async] = {

    implicit val cs = IO.contextShift(ec)
    implicit val timer = IO.timer(ec)
    implicit val fromFuture: FromFuture[IO] = FromFuture.lift[IO]
    implicit val eventual = HeadCache.Eventual[IO](eventualJournal)

    val consumer = {
      val config = consumerConfig.copy(
        autoOffsetReset = AutoOffsetReset.Earliest,
        groupId = None,
        autoCommit = false)

      for {
        kafkaConsumer <- KafkaConsumer.of[IO](config, blocking)
      } yield {
        HeadCache.Consumer[IO](kafkaConsumer)
      }
    }

    val headCache = {
      val headCache = for {
        log       <- Log.of[IO](HeadCache.getClass)
        headCache <- {
          implicit val log1 = log
          HeadCache.of[IO](consumer = consumer)
        }
      } yield headCache

      val timeout = 1.minute
      headCache.unsafeRunTimed(timeout) getOrElse {
        sys.error(s"headCache.unsafeRunTimed timed out in $timeout") // TODO
      }
    }

    new HeadCache[Async] {

      def apply(key: Key, partition: Partition, marker: Offset) = {

        val future = headCache(key, partition, marker).unsafeToFuture()
        Async(future)
      }

      def close = {
        val future = headCache.close.unsafeToFuture()
        Async(future)
      }
    }
  }
}
