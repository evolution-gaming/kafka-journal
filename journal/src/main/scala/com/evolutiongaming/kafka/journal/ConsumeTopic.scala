package com.evolutiongaming.kafka.journal

import cats.effect.{Concurrent, Timer}
import cats.implicits._
import com.evolutiongaming.kafka.journal.HeadCache.Consumer
import com.evolutiongaming.kafka.journal.retry.Retry
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

// TODO Test
object ConsumeTopic {

  def apply[F[_] : Concurrent : Timer : Log](
    topic: Topic,
    from: Map[Partition, Offset],
    pollTimeout: FiniteDuration,
    consumer: F[HeadCache.Consumer[F] /*TODO*/ ])(
    onRecords: ConsumerRecords[String, Bytes] => F[Unit]): F[Unit] = {

    def poll(implicit consumer: Consumer[F]): F[Unit] = {
      for {
        records <- consumer.poll(pollTimeout)
        _ <- {
          if (records.values.isEmpty) {
            ().pure[F]
          } else {
            onRecords(records)
          }
        }
      } yield {}
    }

    def partitionsOf(implicit consumer: Consumer[F]): F[Nel[Partition]] = {
      
      val onError = (error: Throwable, details: Retry.Details) => {
        import Retry.Decision

        def prefix = s"consumer.partitions($topic) failed"

        details.decision match {
          case Decision.Retry(delay) =>
            Log[F].error(s"$prefix, retrying in $delay, error: $error")

          case Decision.GiveUp =>
            val retries = details.retries
            Log[F].error(s"$prefix, retried $retries times, error: $error", error)
        }
      }

      val partitions = for {
        partitions <- consumer.partitions(topic)
        partitions <- Nel.opt(partitions) match {
          case Some(a) => a.pure[F]
          case None    => NoPartitionsException.raiseError[F, Nel[Partition]]
        }
      } yield partitions

      val policy = {
        val fibonacci = Retry.Policy.fibonacci(5.millis)
        Retry.Policy.cap(300.millis, fibonacci)
      }
      Retry(policy, onError)(partitions)
    }

    Consumer.resource(consumer).use { implicit consumer =>
      for {
        partitions <- partitionsOf
        _          <- consumer.assign(topic, partitions)
        offsets = for {
          partition <- partitions
        } yield {
          val offset = from.get(partition).fold(Offset.Min)(_ + 1l)
          (partition, offset)
        }
        _          <- consumer.seek(topic, offsets.toMap)
        _          <- poll.foreverM[Unit]
      } yield {}
    }
  }


  final case object NoPartitionsException extends RuntimeException with NoStackTrace
}
