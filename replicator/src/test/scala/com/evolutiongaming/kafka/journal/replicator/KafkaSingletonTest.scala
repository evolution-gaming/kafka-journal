package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptySet => Nes}
import cats.effect._
import cats.effect.syntax.resource._
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.retry.Sleep
import com.evolutiongaming.skafka.consumer.RebalanceListener
import com.evolutiongaming.skafka.{Partition, TopicPartition}
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class KafkaSingletonTest extends AsyncFunSuite with Matchers {

  test("allocate & release when partition assigned or revoked") {
    `allocate & release when partition assigned or revoked`[IO]().run()
  }

  private def `allocate & release when partition assigned or revoked`[F[_] : Concurrent: Clock: Sleep](): F[Unit] = {

    val topic = "topic"

    def consumer(deferred: Deferred[F, RebalanceListener[F]]) = {
      new TopicConsumer[F] {

        def subscribe(listener: RebalanceListener[F]) = deferred.complete(listener).void

        def poll = Stream.empty

        def commit = TopicCommit.empty
      }
    }

    def topicPartition(partition: Partition) = TopicPartition(topic, partition)

    val result = for {
      listener  <- Deferred[F, RebalanceListener[F]].toResource
      allocated <- Ref[F].of(false).toResource
      resource   = Resource.make { allocated.set(true) } { _ => allocated.set(false) }
      singleton <- KafkaSingleton.of(topic, consumer(listener).pure[Resource[F, *]], resource, Log.empty[F])
      listener  <- listener.get.toResource
      _         <- Resource.eval {
        for {
          a <- singleton.get
          _  = a shouldEqual none[Unit]
          a <- allocated.get
          _  = a shouldEqual false
          _ <- listener.onPartitionsAssigned(Nes.of(topicPartition(Partition.max)))
          a <- singleton.get
          _  = a shouldEqual none[Unit]
          a <- allocated.get
          _  = a shouldEqual false
          _ <- listener.onPartitionsAssigned(Nes.of(topicPartition(Partition.min)))
          _ <- Sleep[F].sleep(10.millis)
          a <- singleton.get
          _  = a shouldEqual ().some
          a <- allocated.get
          _  = a shouldEqual true
          _ <- listener.onPartitionsRevoked(Nes.of(topicPartition(Partition.max)))
          a <- singleton.get
          _  = a shouldEqual ().some
          a <- allocated.get
          _  = a shouldEqual true
          _ <- listener.onPartitionsRevoked(Nes.of(topicPartition(Partition.min)))
          _ <- Sleep[F].sleep(10.millis)
          a <- singleton.get
          _  = a shouldEqual none[Unit]
          a <- allocated.get
          _  = a shouldEqual false
        } yield {}
      }
    } yield {}
    result.use { _ => ().pure[F] }
  }
}
