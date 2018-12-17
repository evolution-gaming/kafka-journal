package com.evolutiongaming.kafka.journal

import cats.effect.{Concurrent, Timer}
import cats.implicits._
import com.evolutiongaming.kafka.journal.HeadCache.Consumer
import com.evolutiongaming.kafka.journal.util.TimerOf
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.concurrent.duration.FiniteDuration

// TODO Test
object ConsumeTopic {

  def apply[F[_] : Concurrent : Timer : Log](
    topic: Topic,
    from: Map[Partition, Offset],
    pollTimeout: FiniteDuration,
    retryInterval: FiniteDuration,
    consumer: F[HeadCache.Consumer[F] /*TODO*/ ])(
    onRecords: ConsumerRecords[String, Bytes] => F[Unit]): F[Unit] = {

    def poll(implicit consumer: Consumer[F]): F[Unit] = {
      for {
        records <- consumer.poll(pollTimeout)
        _       <- {
          if (records.values.isEmpty) {
            ().pure[F]
          } else {
            onRecords(records)
          }
        }
      } yield {}
    }

    def partitionsOf(implicit consumer: Consumer[F]): F[Nel[Partition]] = {
      for {
        partitions <- consumer.partitions(topic).handleErrorWith { error =>
          for {
            _ <- Log[F].warn(s"consumer.partitions($topic) failed, retrying in $retryInterval, error: $error")
          } yield Nil
        }
        partitions <- Nel.opt(partitions) match {
          case Some(a) => a.pure[F]
          case None    => for {
            _          <- TimerOf[F].sleep(retryInterval)
            partitions <- partitionsOf(consumer)
          } yield partitions
        }
      } yield partitions
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
}
