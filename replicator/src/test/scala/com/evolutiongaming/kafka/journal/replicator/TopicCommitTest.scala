package com.evolutiongaming.kafka.journal.replicator

import cats.Applicative
import cats.data.NonEmptyMap as Nem
import cats.effect.{Clock, Deferred, IO, Ref}
import com.evolutiongaming.kafka.journal.IOSuite.*
import com.evolutiongaming.skafka.{Offset, Partition}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class TopicCommitTest extends AsyncFunSuite with Matchers {

  test("delayed") {

    def commitOf(deferred: Deferred[IO, Unit], commitsRef: Ref[IO, List[Nem[Partition, Offset]]])(implicit clock: Clock[IO]) = {
      val commit = new TopicCommit[IO] {
        def apply(offsets: Nem[Partition, Offset]) = {
          commitsRef.update { offsets :: _ } *> deferred.complete(()).void
        }
      }

      TopicCommit.delayed(10.millis, commit)
    }

    def clockOf(ref: Ref[IO, FiniteDuration]): Clock[IO] = {
      new Clock[IO] {
        override def applicative: Applicative[IO] = Applicative[IO]

        override def monotonic: IO[FiniteDuration] = ref.get

        override def realTime: IO[FiniteDuration] = monotonic
      }
    }

    val result = for {
      commitsRef <- Ref[IO].of(List.empty[Nem[Partition, Offset]])
      deferred   <- Deferred[IO, Unit]
      clockRef   <- Ref[IO].of(0.millis)
      clock       = clockOf(clockRef)
      commit     <- commitOf(deferred, commitsRef)(clock)
      _          <- commit(Nem.of((Partition.min, Offset.min)))
      offsets    <- commitsRef.get
      _           = offsets shouldEqual List.empty
      _          <- clockRef.set(20.millis)
      _          <- commit(Nem.of((Partition.unsafe(1), Offset.unsafe(1))))
      _          <- deferred.get
      offsets    <- commitsRef.get
      _           = offsets shouldEqual List(Nem.of((Partition.min, Offset.min), (Partition.unsafe(1), Offset.unsafe(1))))
    } yield {}
    result.run()
  }
}
