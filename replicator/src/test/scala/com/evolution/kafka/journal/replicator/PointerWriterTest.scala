package com.evolution.kafka.journal.replicator

import cats.data.{NonEmptyMap as Nem, NonEmptySet as Nes}
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolution.kafka.journal.eventual.ReplicatedPartitionJournal
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.{Offset, Partition, Topic, TopicPartition}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

class PointerWriterTest extends AnyFreeSpec with Matchers {
  import PointerWriterTest.*

  "PointerWriter" - {

    "writes marked pointers only on a step, then commits and notifies" in {
      val test = for {
        env <- Env.make
        writer <- env.writer
        r0 <- writer.assign(p(0), env.offsets(p(0)), none)
        r1 <- writer.assign(p(1), env.offsets(p(1)), offset(10).some)
        _ <- writer.mark(Nem.of(p(0) -> offset(5), p(1) -> offset(20)), timestamp)
        beforeStep <- env.actions
        _ <- writer.step
        afterStep <- env.actions
        _ <- writer.mark(Nem.of(p(0) -> offset(7)), timestamp)
        _ <- writer.step
        _ <- writer.step
        all <- env.actions
      } yield {
        r0 shouldEqual none
        r1 shouldEqual offset(10).some
        beforeStep shouldEqual Vector.empty
        afterStep.toSet shouldEqual Set(
          Action.Create(p(0), offset(5)),
          Action.Notify(p(0), offset(5)),
          Action.Update(p(1), offset(20)),
          Action.Notify(p(1), offset(20)),
          Action.Commit(Map(p(0) -> offset(6), p(1) -> offset(21))),
        )
        afterStep.last shouldEqual Action.Commit(Map(p(0) -> offset(6), p(1) -> offset(21)))
        all.drop(afterStep.size) shouldEqual Vector(
          Action.Update(p(0), offset(7)),
          Action.Notify(p(0), offset(7)),
          Action.Commit(Map(p(0) -> offset(8))),
        )
      }
      test.unsafeRunSync()
    }

    "commits without a write when the stored pointer is already ahead" in {
      val test = for {
        env <- Env.make
        writer <- env.writer
        _ <- writer.assign(p(0), env.offsets(p(0)), offset(10).some)
        _ <- writer.mark(Nem.of(p(0) -> offset(4)), timestamp)
        _ <- writer.step
        actions <- env.actions
      } yield {
        actions shouldEqual Vector(Action.Commit(Map(p(0) -> offset(5))))
      }
      test.unsafeRunSync()
    }

    "re-assignment keeps the known state and reports marked offsets as replicated" in {
      val test = for {
        env <- Env.make
        writer <- env.writer
        _ <- writer.assign(p(0), env.offsets(p(0)), offset(1).some)
        _ <- writer.mark(Nem.of(p(0) -> offset(4)), timestamp)
        replicated <- writer.assign(p(0), env.offsets(p(0)), offset(1).some)
        _ <- writer.step
        actions <- env.actions
      } yield {
        replicated shouldEqual offset(4).some
        actions shouldEqual Vector(
          Action.Update(p(0), offset(4)),
          Action.Notify(p(0), offset(4)),
          Action.Commit(Map(p(0) -> offset(5))),
        )
      }
      test.unsafeRunSync()
    }

    "ignores marks of unknown partitions" in {
      val test = for {
        env <- Env.make
        writer <- env.writer
        _ <- writer.mark(Nem.of(p(0) -> offset(4)), timestamp)
        _ <- writer.step
        actions <- env.actions
      } yield {
        actions shouldEqual Vector.empty
      }
      test.unsafeRunSync()
    }

    "revoke writes and commits the revoked partitions only, then forgets them" in {
      val test = for {
        env <- Env.make
        writer <- env.writer
        _ <- writer.assign(p(0), env.offsets(p(0)), none)
        _ <- writer.assign(p(1), env.offsets(p(1)), none)
        _ <- writer.mark(Nem.of(p(0) -> offset(5), p(1) -> offset(6)), timestamp)
        _ <- writer.revoke(Nes.of(p(0)))
        afterRevoke <- env.actions
        _ <- writer.mark(Nem.of(p(0) -> offset(9)), timestamp)
        _ <- writer.step
        all <- env.actions
      } yield {
        afterRevoke shouldEqual Vector(
          Action.Create(p(0), offset(5)),
          Action.Notify(p(0), offset(5)),
          Action.Commit(Map(p(0) -> offset(6))),
        )
        all.drop(afterRevoke.size) shouldEqual Vector(
          Action.Create(p(1), offset(6)),
          Action.Notify(p(1), offset(6)),
          Action.Commit(Map(p(1) -> offset(7))),
        )
      }
      test.unsafeRunSync()
    }

    "lose forgets partitions without writing" in {
      val test = for {
        env <- Env.make
        writer <- env.writer
        _ <- writer.assign(p(0), env.offsets(p(0)), none)
        _ <- writer.mark(Nem.of(p(0) -> offset(5)), timestamp)
        _ <- writer.lose(Nes.of(p(0)))
        _ <- writer.step
        actions <- env.actions
      } yield {
        actions shouldEqual Vector.empty
      }
      test.unsafeRunSync()
    }

    "sync writer writes within mark" in {
      val test = for {
        env <- Env.make
        actions <- PointerWriter.Of.sync[IO].createWriter(topic, env.commit, env.notifier).use { writer =>
          for {
            _ <- writer.assign(p(0), env.offsets(p(0)), none)
            _ <- writer.mark(Nem.of(p(0) -> offset(5)), timestamp)
            actions <- env.actions
          } yield actions
        }
      } yield {
        actions shouldEqual Vector(
          Action.Create(p(0), offset(5)),
          Action.Notify(p(0), offset(5)),
          Action.Commit(Map(p(0) -> offset(6))),
        )
      }
      test.unsafeRunSync()
    }

    "periodic writer writes in background and fails the next mark after a failed write" in {
      val test = for {
        env <- Env.make
        result <-
          PointerWriter.Of.periodic[IO](10.millis, Log.empty[IO]).createWriter(topic, env.commit, env.notifier).use {
            writer =>
              for {
                _ <- writer.assign(p(0), env.offsets(p(0)), none)
                _ <- writer.assign(p(1), env.failingOffsets, offset(1).some)
                _ <- writer.mark(Nem.of(p(0) -> offset(5)), timestamp)
                _ <- env.actions.iterateUntil(_.contains(Action.Commit(Map(p(0) -> offset(6))))).timeout(5.seconds)
                _ <- writer.mark(Nem.of(p(1) -> offset(5)), timestamp)
                result <- writer
                  .mark(Nem.of(p(0) -> offset(6)), timestamp)
                  .attempt
                  .iterateUntil(_.isLeft)
                  .timeout(5.seconds)
              } yield result
          }
        actions <- env.actions
      } yield {
        result shouldEqual Left(Error)
        actions should not contain Action.Commit(Map(p(1) -> offset(6)))
      }
      test.unsafeRunSync()
    }
  }
}

object PointerWriterTest {

  val topic: Topic = "topic"

  val timestamp: Instant = Instant.EPOCH

  def p(partition: Int): Partition = Partition.unsafe(partition)

  def offset(offset: Long): Offset = Offset.unsafe(offset)

  case object Error extends RuntimeException with NoStackTrace

  sealed trait Action

  object Action {
    final case class Create(partition: Partition, offset: Offset) extends Action
    final case class Update(partition: Partition, offset: Offset) extends Action
    final case class Notify(partition: Partition, offset: Offset) extends Action
    final case class Commit(offsets: Map[Partition, Offset]) extends Action
  }

  final class Env(actionsRef: Ref[IO, Vector[Action]]) {

    def actions: IO[Vector[Action]] = actionsRef.get

    def offsets(partition: Partition): ReplicatedPartitionJournal.Offsets[IO] =
      new RecordingOffsets(partition, actionsRef)

    def failingOffsets: ReplicatedPartitionJournal.Offsets[IO] = new FailingOffsets

    val commit: TopicCommit[IO] = new RecordingCommit(actionsRef)

    val notifier: ReplicatedOffsetNotifier[IO] = new RecordingNotifier(actionsRef)

    def writer: IO[PointerWriter.Main[IO]] =
      PointerWriter.Main.make[IO](topic, commit, notifier, writeOnMark = false)
  }

  object Env {
    def make: IO[Env] = Ref.of[IO, Vector[Action]](Vector.empty).map { new Env(_) }
  }

  private final class RecordingOffsets(partition: Partition, actionsRef: Ref[IO, Vector[Action]])
  extends ReplicatedPartitionJournal.Offsets[IO] {

    def get: IO[Option[Offset]] = IO.raiseError(new IllegalStateException("not expected"))

    def create(offset: Offset, timestamp: Instant): IO[Unit] =
      actionsRef.update { _ :+ Action.Create(partition, offset) }

    def update(offset: Offset, timestamp: Instant): IO[Unit] =
      actionsRef.update { _ :+ Action.Update(partition, offset) }
  }

  private final class FailingOffsets extends ReplicatedPartitionJournal.Offsets[IO] {

    def get: IO[Option[Offset]] = IO.raiseError(Error)

    def create(offset: Offset, timestamp: Instant): IO[Unit] = IO.raiseError(Error)

    def update(offset: Offset, timestamp: Instant): IO[Unit] = IO.raiseError(Error)
  }

  private final class RecordingCommit(actionsRef: Ref[IO, Vector[Action]]) extends TopicCommit[IO] {

    def apply(offsets: Nem[Partition, Offset]): IO[Unit] =
      actionsRef.update { _ :+ Action.Commit(offsets.toSortedMap) }
  }

  private final class RecordingNotifier(actionsRef: Ref[IO, Vector[Action]]) extends ReplicatedOffsetNotifier[IO] {

    def onReplicatedOffset(topicPartition: TopicPartition, offset: Offset): IO[Unit] =
      actionsRef.update { _ :+ Action.Notify(topicPartition.partition, offset) }
  }
}
