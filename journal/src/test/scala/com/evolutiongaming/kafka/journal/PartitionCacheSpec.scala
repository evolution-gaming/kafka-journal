package com.evolutiongaming.kafka.journal

import cats.data.NonEmptyList as Nel
import cats.effect.IO
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.IOSuite.*
import com.evolutiongaming.kafka.journal.PartitionCache.*
import com.evolutiongaming.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.skafka.Offset
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.concurrent.TimeoutException
import scala.concurrent.duration.*
import scala.util.Try

class PartitionCacheSpec extends AsyncFunSuite with Matchers {
  import PartitionCacheSpec.*

  private def partitionCacheOf(
    maxSize: Int            = 10,
    dropUponLimit: Double   = 0.1,
    timeout: FiniteDuration = 1.minute,
  ) = {
    PartitionCache.make[IO](maxSize, dropUponLimit, timeout)
  }

  test("get Result.empty when cache is empty") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case _: Result.Later.Empty[IO] => () }
        } yield a
      }
      .run()
  }

  test("get Result.ahead when offset equals request") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.remove(offset0)
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset0)
          _ <- IO { a shouldEqual Result.ahead }
        } yield {}
      }
      .run()
  }

  test("get Result.ahead when offset is greater than request") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.remove(offset1)
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset0)
          _ <- IO { a shouldEqual Result.ahead }
        } yield {}
      }
      .run()
  }

  test("get Result.empty when offset is smaller than request") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.remove(offset0)
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case _: Result.Later.Empty[IO] => () }
        } yield a
      }
      .run()
  }

  test("get HeadInfo.empty") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.add(Record(id0, offset0, actionHeader))
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset0)
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield {}
      }
      .run()
  }

  test("get HeadInfo.empty if no entries") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.remove(offset0)
          _ <- IO { a shouldEqual none }
          a <- cache.add(Record(offset1, none))
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset1)
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield a
      }
      .run()
  }

  test("get HeadInfo.append") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.add(Record(id0, offset1, actionHeaderOf(seqNr0)))
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset1)
          _ <- IO { a shouldEqual Result.value(HeadInfo.append(offset1, seqNr0, none)) }
        } yield {}
      }
      .run()
  }

  test("get HeadInfo.delete") {
    partitionCacheOf()
      .use { cache =>
        val actionHeader = ActionHeader.Delete(to = DeleteTo(seqNr0), origin = none, version = none)
        for {
          a <- cache.add(Record(id0, offset0, actionHeader))
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset0)
          _ <- IO { a shouldEqual Result.value(HeadInfo.delete(DeleteTo(seqNr0))) }
        } yield {}
      }
      .run()
  }

  test("get Result.Later.Behind[IO] and add same offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.add(Record(id0, offset0, actionHeader))
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case a: Result.Later.Behind[IO] => a.value }
          b <- cache.add(Record(id0, offset1, actionHeader))
          _ <- IO { b shouldEqual Diff(1).some }
          a <- a
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield {}
      }
      .run()
  }

  test("get Result.Later.Behind[IO] and add same offset with different id") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.add(Record(id0, offset0, actionHeader))
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case a: Result.Later.Behind[IO] => a.value }
          b <- cache.add(Record(id1, offset1, actionHeader))
          _ <- IO { b shouldEqual Diff(1).some }
          a <- a
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield {}
      }
      .run()
  }

  test("get Result.Later.Behind[IO] and add greater offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.add(Record(id0, offset0, actionHeader))
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case a: Result.Later.Behind[IO] => a.value }
          b <- cache.add(Record(id0, offset2, actionHeader))
          _ <- IO { b shouldEqual Diff(2).some }
          a <- a
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield {}
      }
      .run()
  }

  test("get Result.Later.Behind[IO] and add greater offset with different id") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.add(Record(id0, offset0, actionHeader))
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case a: Result.Later.Behind[IO] => a.value }
          b <- cache.add(Record(id1, offset2, actionHeader))
          _ <- IO { b shouldEqual Diff(2).some }
          a <- a
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield {}
      }
      .run()
  }

  test("get Result.Later.Behind[IO] and add smaller offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.add(Record(id0, offset0, actionHeader))
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset2)
          a <- a.matchOrError { case a: Result.Later.Behind[IO] => a.value }
          b <- cache.add(Record(id0, offset1, actionHeader))
          _ <- IO { b shouldEqual Diff(1).some }
          b <- a.timeout(10.millis).attempt
          _ <- IO { b should matchPattern { case Left(_: TimeoutException) => } }
          b <- cache.add(Record(id0, offset2, actionHeader))
          _ <- IO { b shouldEqual Diff(1).some }
          a <- a
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield {}
      }
      .run()
  }

  test("get Result.Later.Behind[IO] and add smaller offset with different id") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.add(Record(id0, offset0, actionHeader))
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset2)
          a <- a.matchOrError { case a: Result.Later.Behind[IO] => a.value }
          b <- cache.add(Record(id1, offset1, actionHeader))
          _ <- IO { b shouldEqual Diff(1).some }
          b <- a.timeout(10.millis).attempt
          _ <- IO { b should matchPattern { case Left(_: TimeoutException) => } }
          b <- cache.add(Record(id1, offset2, actionHeader))
          _ <- IO { b shouldEqual Diff(1).some }
          a <- a
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield {}
      }
      .run()
  }

  test("get Result.empty and add same offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.get(id0, offset0)
          a <- a.matchOrError { case a: Result.Later.Empty[IO] => a.value }
          b <- cache.add(Record(id0, offset0, actionHeader))
          _ <- IO { b shouldEqual none }
          a <- a
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield {}
      }
      .run()
  }

  test("get Result.empty and add same offset with different id") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.get(id0, offset0)
          a <- a.matchOrError { case a: Result.Later.Empty[IO] => a.value }
          b <- cache.add(Record(id1, offset0, actionHeader))
          _ <- IO { b shouldEqual none }
          a <- a
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield {}
      }
      .run()
  }

  test("get Result.empty and add greater offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.get(id0, offset0)
          a <- a.matchOrError { case a: Result.Later.Empty[IO] => a.value }
          b <- cache.add(Record(id0, offset1, actionHeader))
          _ <- IO { b shouldEqual none }
          a <- a
          _ <- IO { a shouldEqual Result.limited }
        } yield {}
      }
      .run()
  }

  test("get Result.empty and add greater offset with different id") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.get(id0, offset0)
          a <- a.matchOrError { case a: Result.Later.Empty[IO] => a.value }
          d <- cache.add(Record(id1, offset1, actionHeader))
          _ <- IO { d shouldEqual none }
          a <- a
          _ <- IO { a shouldEqual Result.limited }
        } yield {}
      }
      .run()
  }

  test("get Result.empty and add smaller offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case a: Result.Later.Empty[IO] => a.value }
          _ <- cache.add(Record(id0, offset0, actionHeader))
          b <- a.timeout(10.millis).attempt
          _ <- IO { b should matchPattern { case Left(_: TimeoutException) => } }
          _ <- cache.add(Record(id0, offset1, actionHeader))
          a <- a
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield {}
      }
      .run()
  }

  test("get Result.empty and add smaller offset with different id") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case a: Result.Later.Empty[IO] => a.value }
          _ <- cache.add(Record(id1, offset0, actionHeader))
          b <- a.timeout(10.millis).attempt
          _ <- IO { b should matchPattern { case Left(_: TimeoutException) => } }
          _ <- cache.add(Record(id1, offset1, actionHeader))
          a <- a
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield {}
      }
      .run()
  }

  test("get Result.empty and remove same offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.get(id0, offset0)
          a <- a.matchOrError { case a: Result.Later.Empty[IO] => a.value }
          _ <- cache.remove(offset0)
          a <- a
          _ <- IO { a shouldEqual Result.ahead }
        } yield {}
      }
      .run()
  }

  test("get Result.empty and remove greater offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.get(id0, offset0)
          a <- a.matchOrError { case a: Result.Later.Empty[IO] => a.value }
          _ <- cache.remove(offset1)
          a <- a
          _ <- IO { a shouldEqual Result.ahead }
        } yield {}
      }
      .run()
  }

  test("get Result.empty and remove smaller offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case a: Result.Later.Empty[IO] => a.value }
          _ <- cache.remove(offset0)
          b <- a.timeout(10.millis).attempt
          _ <- IO { b should matchPattern { case Left(_: TimeoutException) => } }
          _ <- cache.remove(offset1)
          a <- a
          _ <- IO { a shouldEqual Result.ahead }
        } yield {}
      }
      .run()
  }

  test("get Result.Later.Behind[IO] and remove same offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          _ <- cache.add(Record(id0, offset0, actionHeader))
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case a: Result.Later.Behind[IO] => a.value }
          _ <- cache.remove(offset1)
          a <- a
          _ <- IO { a shouldEqual Result.ahead }
        } yield {}
      }
      .run()
  }

  test("get Result.Later.Behind[IO] and remove greater offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          _ <- cache.add(Record(id0, offset0, actionHeader))
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case a: Result.Later.Behind[IO] => a.value }
          _ <- cache.remove(offset2)
          a <- a
          _ <- IO { a shouldEqual Result.ahead }
        } yield {}
      }
      .run()
  }

  test("get Result.Later.Behind[IO] and remove smaller offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          _ <- cache.add(Record(id0, offset0, actionHeader))
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case a: Result.Later.Behind[IO] => a.value }
          _ <- cache.remove(offset0)
          b <- a.timeout(10.millis).attempt
          _ <- IO { b should matchPattern { case Left(_: TimeoutException) => } }
          _ <- cache.remove(offset1)
          a <- a
          _ <- IO { a shouldEqual Result.ahead }
        } yield {}
      }
      .run()
  }

  test("get Result.limited") {
    partitionCacheOf(maxSize = 2, dropUponLimit = 0.5)
      .use { cache =>
        for {
          a <- cache.add(Record(id0, offset0, actionHeaderOf(seqNr0)))
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset0)
          _ <- IO { a shouldEqual Result.value(HeadInfo.append(offset0, seqNr0, none)) }
          a <- cache.add(Record(id0, offset1, actionHeaderOf(seqNr1)))
          _ <- IO { a shouldEqual Diff(1).some }
          a <- cache.get(id0, offset1)
          _ <- IO { a shouldEqual Result.value(HeadInfo.append(offset0, seqNr1, none)) }
          a <- cache.add(Record(id1, offset2, actionHeaderOf(seqNr0)))
          _ <- IO { a shouldEqual Diff(1).some }
          a <- cache.get(id1, offset2)
          _ <- IO { a shouldEqual Result.value(HeadInfo.append(offset2, seqNr0, none)) }
          a <- cache.add(Record(id2, offset3, actionHeaderOf(seqNr0)))
          _ <- IO { a shouldEqual Diff(1).some }
          a <- cache.get(id2, offset3)
          _ <- IO { a shouldEqual Result.value(HeadInfo.append(offset3, seqNr0, none)) }
          a <- cache.get(id0, offset2)
          _ <- IO { a shouldEqual Result.limited }
        } yield {}
      }
      .run()
  }

  test("get Result.limited when dropUponLimit = 1.0") {
    partitionCacheOf(maxSize = 2, dropUponLimit = 1.0)
      .use { cache =>
        for {
          a <- cache.add(Record(id0, offset0, actionHeaderOf(seqNr0)))
          _ <- IO { a shouldEqual none }
          a <- cache.add(Record(id1, offset1, actionHeaderOf(seqNr0)))
          _ <- IO { a shouldEqual Diff(1).some }
          a <- cache.add(Record(id2, offset2, actionHeaderOf(seqNr0)))
          _ <- IO { a shouldEqual Diff(1).some }
          a <- cache.get(id0, offset0)
          _ <- IO { a shouldEqual Result.limited }
          a <- cache.get(id1, offset1)
          _ <- IO { a shouldEqual Result.limited }
          a <- cache.get(id2, offset2)
          _ <- IO { a shouldEqual Result.value(HeadInfo.append(offset2, seqNr0, none)) }
        } yield {}
      }
      .run()
  }

  test("get Result.limited after append of oversized batch") {
    partitionCacheOf(maxSize = 2, dropUponLimit = 0.5)
      .use { cache =>
        for {
          a <- cache.get(id0, offset0)
          a <- a.matchOrError { case a: Result.Later.Empty[IO] => a.value }
          d <- cache.add(
            Nel.of(
              Record(id0, offset0, actionHeaderOf(seqNr0)),
              Record(id1, offset1, actionHeaderOf(seqNr0)),
              Record(id2, offset2, actionHeaderOf(seqNr0)),
            ),
          )
          _ <- IO { d shouldEqual none }
          a <- a
          _ <- IO { a shouldEqual Result.value(HeadInfo.append(offset0, seqNr0, none)) }
          a <- cache.get(id0, offset0)
          _ <- IO { a shouldEqual Result.limited }
          a <- cache.get(id1, offset0)
          _ <- IO { a shouldEqual Result.limited }
          a <- cache.get(id2, offset2)
          _ <- IO { a shouldEqual Result.value(HeadInfo.append(offset2, seqNr0, none)) }
        } yield {}
      }
      .run()
  }

  test("get Result.value even if within limited range") {
    partitionCacheOf(maxSize = 2, dropUponLimit = 0.4)
      .use { cache =>
        for {
          a <- cache.add(
            Nel.of(
              Record(id0, offset0, actionHeaderOf(seqNr0)),
              Record(id1, offset1, actionHeaderOf(seqNr0)),
              Record(id0, offset2, actionHeaderOf(seqNr1)),
              Record(id2, offset3, actionHeaderOf(seqNr0)),
            ),
          )
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset0)
          _ <- IO { a shouldEqual Result.value(HeadInfo.append(offset0, seqNr1, none)) }
          a <- cache.get(id1, offset1)
          _ <- IO { a shouldEqual Result.limited }
          a <- cache.get(id2, offset3)
          _ <- IO { a shouldEqual Result.value(HeadInfo.append(offset3, seqNr0, none)) }
        } yield {}
      }
      .run()
  }

  test("marks do not impact limits") {
    partitionCacheOf(maxSize = 2, dropUponLimit = 0.5)
      .use { cache =>
        for {
          a <- cache.add(
            Nel.of(
              Record(id0, offset0, actionHeader),
              Record(id1, offset1, actionHeader),
              Record(id2, offset2, actionHeader),
            ),
          )
          _ <- IO { a shouldEqual none }
          a <- cache.get(id0, offset0)
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
          a <- cache.get(id2, offset1)
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
          a <- cache.get(id0, offset2)
          _ <- IO { a shouldEqual Result.value(HeadInfo.empty) }
        } yield {}
      }
      .run()
  }

  test("offset") {
    partitionCacheOf()
      .use { cache =>
        for {
          a <- cache.offset
          _ <- IO { a shouldEqual none }
          a <- cache.remove(offset1)
          _ <- IO { a shouldEqual none }
          a <- cache.offset
          _ <- IO { a shouldEqual offset1.some }
          a <- cache.remove(offset0)
          _ <- IO { a shouldEqual none }
          a <- cache.offset
          _ <- IO { a shouldEqual offset1.some }
          a <- cache.add(Record(id0, offset2, actionHeader))
          _ <- IO { a shouldEqual none }
          a <- cache.offset
          _ <- IO { a shouldEqual offset2.some }
          a <- cache.remove(offset3)
          _ <- IO { a shouldEqual Diff(2).some }
          a <- cache.offset
          _ <- IO { a shouldEqual offset3.some }
        } yield {}
      }
      .run()
  }

  test("timeout when empty") {
    partitionCacheOf(timeout = 10.millis)
      .use { cache =>
        for {
          a <- cache.get(id0, offset0)
          a <- a.matchOrError { case a: Result.Later.Empty[IO] => a.value }
          a <- a
          _ <- IO { a shouldEqual Result.Now.timeout(10.millis) }
        } yield {}
      }
      .run()

  }

  test("timeout when behind") {
    partitionCacheOf(timeout = 10.millis)
      .use { cache =>
        for {
          _ <- cache.add(Record(id0, offset0, actionHeader))
          a <- cache.get(id0, offset1)
          a <- a.matchOrError { case a: Result.Later.Behind[IO] => a.value }
          a <- a
          _ <- IO { a shouldEqual Result.Now.timeout(10.millis) }
        } yield {}
      }
      .run()
  }

  test("release") {
    val result = for {
      value <- partitionCacheOf().use { cache =>
        cache
          .get(id0, offset0)
          .map { result => (cache, result) }
      }
      (cache, a) = value
      a         <- a.toNow.attempt
      _         <- IO { a shouldEqual ReleasedError.asLeft }
      a         <- cache.get(id0, offset0).attempt
      _         <- IO { a shouldEqual ReleasedError.asLeft }
      a         <- cache.add(Record(id0, offset0, actionHeader)).attempt
      _         <- IO { a shouldEqual ReleasedError.asLeft }
      a         <- cache.remove(offset0).attempt
      _         <- IO { a shouldEqual ReleasedError.asLeft }
    } yield {}
    result.run()
  }
}

object PartitionCacheSpec {
  val timestamp: Instant = Instant.now()
  val offset0: Offset    = Offset.min
  val offset1: Offset    = offset0.inc[Try].get
  val offset2: Offset    = offset1.inc[Try].get
  val offset3: Offset    = offset2.inc[Try].get
  val id0: String        = "id0"
  val id1: String        = "id1"
  val id2: String        = "id2"
  val seqNr0: SeqNr      = SeqNr.min
  val seqNr1: SeqNr      = seqNr0.next[Try].get

  val actionHeader: ActionHeader = ActionHeader.Mark("mark", none, none)

  def actionHeaderOf(seqNr: SeqNr): ActionHeader.Append = {
    ActionHeader.Append(
      range       = SeqRange(seqNr),
      origin      = none,
      payloadType = PayloadType.Binary,
      metadata    = HeaderMetadata.empty,
      version     = none,
    )
  }

  private implicit class Ops[A](val self: A) extends AnyVal {
    def matchOrError[B](pf: PartialFunction[A, B]): IO[B] = {
      pf.lift(self) match {
        case Some(a) =>
          a.pure[IO]
        case None =>
          new MatchError(self).raiseError[IO, B]
      }
    }
  }
}
