package com.evolutiongaming.kafka.journal

import cats.effect.IO
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.util.IOFromFuture
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, Consumer, ConsumerConfig}
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

object HeadCacheAsync {

  def apply(
    consumerConfig: ConsumerConfig,
    eventualJournal: EventualJournal[Async],
    ecBlocking: ExecutionContext,
    actorLog: ActorLog)(implicit ec: ExecutionContext): HeadCache[com.evolutiongaming.concurrent.async.Async] = {

    implicit val cs = IO.contextShift(ec)
    implicit val timer = IO.timer(ec)
    implicit val log = Log.fromLog[IO](actorLog)
    implicit val eventual = new HeadCache.Eventual[IO] {

      def pointers(topic: Topic) = {
        IOFromFuture {
          eventualJournal.pointers(topic).future
        }
      }
    }

    val consumer = IO.delay {
      val config = consumerConfig.copy(
        autoOffsetReset = AutoOffsetReset.Earliest,
        groupId = None)

      val consumer = Consumer[Id, Bytes](config, ecBlocking)
      HeadCache.Consumer(consumer)
    }

    val headCache = {
      val headCache = HeadCache.of[IO](
        consumer = consumer).unsafeToFuture()
      Await.result(headCache, 10.seconds) // TODO
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
