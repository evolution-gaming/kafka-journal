package com.evolutiongaming.kafka.journal

import cats.effect.{ContextShift, IO}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, Consumer, ConsumerConfig}
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object HeadCacheAsync {

  def apply(
    consumerConfig: ConsumerConfig,
    eventualJournal: EventualJournal[Async],
    ecBlocking: ExecutionContext)(implicit
    ec: ExecutionContextExecutor): HeadCache[com.evolutiongaming.concurrent.async.Async] = {

    implicit val cs = IO.contextShift(ec)
    implicit val timer = IO.timer(ec)
    implicit val fromFuture: FromFuture[IO] = FromFuture.lift[IO]
    implicit val eventual = new HeadCache.Eventual[IO] {

      def pointers(topic: Topic) = {
        fromFuture {
          eventualJournal.pointers(topic).future
        }
      }
    }

    val consumer = {
      val config = consumerConfig.copy(
        autoOffsetReset = AutoOffsetReset.Earliest,
        groupId = None,
        autoCommit = false)

      for {
        consumer <- ContextShift[IO].evalOn(ecBlocking) {
          IO.delay {
            Consumer[Id, Bytes](config, ecBlocking)
          }
        }
      } yield {
        HeadCache.Consumer[IO](consumer)
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

    new HeadCache[com.evolutiongaming.concurrent.async.Async] {

      def apply(key: Key, partition: Partition, marker: Offset) = {

        val future = headCache(key, partition, marker).unsafeToFuture()
        com.evolutiongaming.concurrent.async.Async(future)
      }

      def close = {
        val future = headCache.close.unsafeToFuture()
        com.evolutiongaming.concurrent.async.Async(future)
      }
    }
  }
}
