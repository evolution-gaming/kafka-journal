package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel}
import cats.effect.concurrent.{Deferred, Ref}
import cats.effect.{Concurrent, IO, Resource, Timer}
import cats.implicits._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.journal.IOSuite._
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

  private def `allocate & release when partition assigned or revoked`[F[_] : Concurrent : Timer](): F[Unit] = {

    val topic = "topic"

    def consumer(deferred: Deferred[F, RebalanceListener[F]]) = {
      new TopicConsumer[F] {

        def subscribe(listener: RebalanceListener[F]) = deferred.complete(listener)

        def poll = Stream.empty

        def commit = TopicCommit.empty
      }
    }

    def topicPartition(partition: Partition) = TopicPartition(topic, partition)

    val result = for {
      listener  <- Resource.liftF(Deferred[F, RebalanceListener[F]])
      allocated <- Resource.liftF(Ref[F].of(false))
      resource   = Resource.make { allocated.set(true) } { _ => allocated.set(false) }
      singleton <- KafkaSingleton.of(topic, consumer(listener).pure[Resource[F, *]], resource, Log.empty[F])
      listener  <- Resource.liftF(listener.get)
      _         <- Resource.liftF {
        for {
          a <- singleton.get
          _  = a shouldEqual none[Unit]
          a <- allocated.get
          _  = a shouldEqual false
          _ <- listener.onPartitionsAssigned(Nel.of(topicPartition(Partition.max)))
          a <- singleton.get
          _  = a shouldEqual none[Unit]
          a <- allocated.get
          _  = a shouldEqual false
          _ <- listener.onPartitionsAssigned(Nel.of(topicPartition(Partition.min)))
          _ <- Timer[F].sleep(10.millis)
          a <- singleton.get
          _  = a shouldEqual ().some
          a <- allocated.get
          _  = a shouldEqual true
          _ <- listener.onPartitionsRevoked(Nel.of(topicPartition(Partition.max)))
          a <- singleton.get
          _  = a shouldEqual ().some
          a <- allocated.get
          _  = a shouldEqual true
          _ <- listener.onPartitionsRevoked(Nel.of(topicPartition(Partition.min)))
          _ <- Timer[F].sleep(10.millis)
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
