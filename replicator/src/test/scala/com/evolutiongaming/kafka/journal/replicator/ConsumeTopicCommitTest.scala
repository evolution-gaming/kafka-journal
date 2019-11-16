package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyMap => Nem}
import cats.implicits._
import cats.effect.{IO, Timer}
import com.evolutiongaming.kafka.journal.IOSuite._
import cats.effect.concurrent.{Deferred, Ref}
import com.evolutiongaming.skafka.{Offset, Partition}
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.duration._

class ConsumeTopicCommitTest extends AsyncFunSuite with Matchers{

  test("delayed") {
    val result = for {
      commitsRef <- Ref[IO].of(List.empty[Nem[Partition, Offset]])
      deferred   <- Deferred[IO, Unit]
      commit = new ConsumeTopic.Commit[IO] {
        def apply(offsets: Nem[Partition, Offset]) = {
          commitsRef.update { offsets :: _ } *> deferred.complete(())
        }
      }
      commit  <- ConsumeTopic.Commit.delayed(100.millis, commit)
      _       <- commit(Nem.of((0, 0L)))
      offsets <- commitsRef.get
      _        = offsets shouldEqual List.empty
      _       <- Timer[IO].sleep(200.millis)
      _       <- commit(Nem.of((1, 1L)))
      _       <- deferred.get
      offsets <- commitsRef.get
      _        = offsets shouldEqual List(Nem.of((0, 0L), (1, 1L)))
    } yield {}
    result.run()
  }
}
