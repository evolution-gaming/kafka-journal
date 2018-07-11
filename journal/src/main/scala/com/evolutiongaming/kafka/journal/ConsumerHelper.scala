package com.evolutiongaming.kafka.journal

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecords}

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

object ConsumerHelper {

  implicit class ConsumerOps[K, V](val self: Consumer[K, V]) extends AnyVal {

    // TODO stop consumer
    def source[S, E](
      s: S,
      timeout: FiniteDuration)(
      f: (S, ConsumerRecords[K, V]) => (S, Boolean, Seq[E]))(implicit
      ec: ExecutionContext /*TODO*/): Source[E, NotUsed] = {

      Source.foldWhile(s) { s =>
        for {
          records <- self.poll(timeout)
        } yield {
          f(s, records)
        }
      }
    }

    // TODO FastFuture
    // TODO rename
    def fold[S](s: S, timeout: FiniteDuration)(
      f: (S, ConsumerRecords[K, V]) => (S, Boolean))(implicit
      ec: ExecutionContext /*TODO*/): Future[S] = {

      val ff = (s: S) => for {
        records <- self.poll(timeout)
      } yield {
        f(s, records)
      }

      ff.foldWhile(s)
    }

    // TODO FastFuture
    // TODO rename
    def foldAsync[S](s: S, timeout: FiniteDuration)(
      f: (S, ConsumerRecords[K, V]) => Future[(S, Boolean)])(implicit
      ec: ExecutionContext /*TODO*/): Future[S] = {

      val ff = (s: S) => for {
        records <- self.poll(timeout)
        result <- f(s, records)
      } yield {
        result
      }

      ff.foldWhile(s)
    }
  }
}
