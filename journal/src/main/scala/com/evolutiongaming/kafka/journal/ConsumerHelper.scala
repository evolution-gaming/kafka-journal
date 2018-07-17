package com.evolutiongaming.kafka.journal

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecords}

import scala.collection.immutable.Seq
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext

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
  }
}
