package com.evolutiongaming.kafka.journal.replicator

import cats.data.NonEmptySet as Nes
import cats.effect.*
import cats.effect.syntax.resource.*
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{Log, ToTry}
import com.evolutiongaming.kafka.journal.IOSuite.*
import com.evolutiongaming.retry.Sleep
import com.evolutiongaming.skafka.consumer.RebalanceListener1
import com.evolutiongaming.skafka.{Partition, TopicPartition}
import com.evolutiongaming.sstream.Stream
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class KafkaSingletonTest extends AsyncFunSuite with Matchers {

  test("allocate & release when partition assigned or revoked") {
    `allocate & release when partition assigned or revoked`[IO]().run()
  }

  private def `allocate & release when partition assigned or revoked`[F[_]: Concurrent: Sleep: ToTry](): F[Unit] = {

    val topic = "topic"

    def consumer(deferred: Deferred[F, RebalanceListener1[F]]) = {
      new TopicConsumer[F] {

        def subscribe(listener: RebalanceListener1[F]) = deferred.complete(listener).void

        def poll = Stream.empty

        def commit = TopicCommit.empty
      }
    }

    def topicPartition(partition: Partition) = TopicPartition(topic, partition)

    val result = for {
      listener  <- Deferred[F, RebalanceListener1[F]].toResource
      allocated <- Ref[F].of(false).toResource
      resource   = Resource.make { allocated.set(true) } { _ => allocated.set(false) }
      singleton <- KafkaSingleton.make(topic, consumer(listener).pure[Resource[F, *]], resource, Log.empty[F])
      listener  <- listener.get.toResource
      _ <- Resource.eval {
        for {
          a <- singleton.get
          _  = a shouldEqual none[Unit]
          a <- allocated.get
          _  = a shouldEqual false
          _ <- listener.onPartitionsAssigned(Nes.of(topicPartition(Partition.max))).run(EmptyRebalanceConsumer).liftTo[F]
          a <- singleton.get
          _  = a shouldEqual none[Unit]
          a <- allocated.get
          _  = a shouldEqual false
          _ <- listener.onPartitionsAssigned(Nes.of(topicPartition(Partition.min))).run(EmptyRebalanceConsumer).liftTo[F]
          _ <- Sleep[F].sleep(100.millis)
          a <- singleton.get
          _  = a shouldEqual ().some
          a <- allocated.get
          _  = a shouldEqual true
          _ <- listener.onPartitionsRevoked(Nes.of(topicPartition(Partition.max))).run(EmptyRebalanceConsumer).liftTo[F]
          a <- singleton.get
          _  = a shouldEqual ().some
          a <- allocated.get
          _  = a shouldEqual true
          _ <- listener.onPartitionsRevoked(Nes.of(topicPartition(Partition.min))).run(EmptyRebalanceConsumer).liftTo[F]
          _ <- Sleep[F].sleep(100.millis)
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
