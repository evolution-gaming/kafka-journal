package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyMap => Nem}
import cats.implicits._
import cats.effect.{Clock, IO}
import com.evolutiongaming.kafka.journal.IOSuite._
import cats.effect.concurrent.{Deferred, Ref}
import com.evolutiongaming.skafka.{Offset, Partition}
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class TopicCommitTest extends AsyncFunSuite with Matchers{

  test("delayed") {

    def commitOf(
      deferred: Deferred[IO, Unit],
      commitsRef: Ref[IO, List[Nem[Partition, Offset]]])(implicit
      clock: Clock[IO]
    ) = {
      val commit = new TopicCommit[IO] {
        def apply(offsets: Nem[Partition, Offset]) = {
          commitsRef.update { offsets :: _ } *> deferred.complete(())
        }
      }

      TopicCommit.delayed(10.millis, commit)
    }

    def clockOf(ref: Ref[IO, FiniteDuration]): Clock[IO] = {
      new Clock[IO] {
        def realTime(unit: TimeUnit): IO[Long] = monotonic(unit)
        def monotonic(unit: TimeUnit): IO[Long] = ref.get.map { _.toUnit(unit).toLong }
      }
    }

    val result = for {
      commitsRef <- Ref[IO].of(List.empty[Nem[Partition, Offset]])
      deferred   <- Deferred[IO, Unit]
      clockRef   <- Ref[IO].of(0.millis)
      clock       = clockOf(clockRef)
      commit     <- commitOf(deferred, commitsRef)(clock)
      _          <- commit(Nem.of((0, 0L)))
      offsets    <- commitsRef.get
      _           = offsets shouldEqual List.empty
      _          <- clockRef.set(20.millis)
      _          <- commit(Nem.of((1, 1L)))
      _          <- deferred.get
      offsets    <- commitsRef.get
      _           = offsets shouldEqual List(Nem.of((0, 0L), (1, 1L)))
    } yield {}
    result.run()
  }
}
