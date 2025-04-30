package com.evolutiongaming.kafka.journal.replicator

import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.skafka.Offset
import org.openjdk.jmh.annotations.{Benchmark, Fork, Measurement, Scope, State, Warmup}
import org.openjdk.jmh.infra.Blackhole
import scodec.bits.ByteVector

import java.time.Instant
import java.util.concurrent.TimeUnit

/**
 * To run benchmarks:
 * {{{sbt benchmark/Jmh/run com.evolutiongaming.kafka.journal.replicator.BatchBenchmark}}}
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
class BatchBenchmark {

  @Benchmark
  def original4_1_0(blackhole: Blackhole): Unit = {
    val of = Batch_4_1_0.of
    callAll[Batch_4_1_0](blackhole, of)
  }

  @Benchmark
  def alternativeWithVector(blackhole: Blackhole): Unit = {
    val of = Batch_4_1_0_Alternative_with_Vector.of
    callAll[Batch_4_1_0_Alternative_with_Vector](blackhole, of)
  }

  @Benchmark
  def alternativeWithAggressiveReshuffling(blackhole: Blackhole): Unit = {
    val of = Batch_Alternative_with_Aggressive_Reshuffling.of
    callAll[Batch_Alternative_with_Aggressive_Reshuffling](blackhole, of)
  }

  @Benchmark
  def alternative4_1_2(blackhole: Blackhole): Unit = {
    val of = Batch_4_1_2.of
    callAll[Batch_4_1_2](blackhole, of)
  }

  def callAll[T](blackhole: Blackhole, of: (Nel[ActionRecord[Action]]) => List[T]): Unit = {
    blackhole.consume(of(b1.map(actionRecordOf)))
    blackhole.consume(of(b2.map(actionRecordOf)))
    blackhole.consume(of(b3.map(actionRecordOf)))
    blackhole.consume(of(b4.map(actionRecordOf)))
    blackhole.consume(of(b5.map(actionRecordOf)))
    blackhole.consume(of(b6.map(actionRecordOf)))
    blackhole.consume(of(b7.map(actionRecordOf)))
    blackhole.consume(of(b8.map(actionRecordOf)))
    blackhole.consume(of(b9.map(actionRecordOf)))
    blackhole.consume(of(b10.map(actionRecordOf)))
    blackhole.consume(of(b11.map(actionRecordOf)))
    blackhole.consume(of(b12.map(actionRecordOf)))
    blackhole.consume(of(b13.map(actionRecordOf)))
    blackhole.consume(of(b14.map(actionRecordOf)))
    blackhole.consume(of(b15.map(actionRecordOf)))
    blackhole.consume(of(b16.map(actionRecordOf)))
    blackhole.consume(of(b17.map(actionRecordOf)))
    blackhole.consume(of(b18.map(actionRecordOf)))
    blackhole.consume(of(b19.map(actionRecordOf)))
    blackhole.consume(of(b20.map(actionRecordOf)))
    blackhole.consume(of(b21.map(actionRecordOf)))
    blackhole.consume(of(b22.map(actionRecordOf)))
    blackhole.consume(of(b23.map(actionRecordOf)))
    blackhole.consume(of(b24.map(actionRecordOf)))
    blackhole.consume(of(b25.map(actionRecordOf)))
    blackhole.consume(of(b26.map(actionRecordOf)))
    blackhole.consume(of(b27.map(actionRecordOf)))
    blackhole.consume(of(b28.map(actionRecordOf)))
    blackhole.consume(of(b29.map(actionRecordOf)))
    blackhole.consume(of(b30.map(actionRecordOf)))
    blackhole.consume(of(b31.map(actionRecordOf)))
    blackhole.consume(of(b32.map(actionRecordOf)))
    blackhole.consume(of(b33.map(actionRecordOf)))
    blackhole.consume(of(b34.map(actionRecordOf)))
    blackhole.consume(of(b35.map(actionRecordOf)))
    blackhole.consume(of(b36.map(actionRecordOf)))
    blackhole.consume(of(b37.map(actionRecordOf)))
    blackhole.consume(of(b38.map(actionRecordOf)))
    blackhole.consume(of(b39.map(actionRecordOf)))
    blackhole.consume(of(b30.map(actionRecordOf)))
    blackhole.consume(of(b41.map(actionRecordOf)))
    blackhole.consume(of(b42.map(actionRecordOf)))
    blackhole.consume(of(b43.map(actionRecordOf)))
    blackhole.consume(of(b44.map(actionRecordOf)))
    blackhole.consume(of(b45.map(actionRecordOf)))
  }

  def b1: Nel[A] = Nel.of(mark(offset = 0))
  def b2: Nel[A] = Nel.of(mark(offset = 0), mark(offset = 1))
  def b3: Nel[A] = Nel.of(append(offset = 0, seqNr = 1))
  def b4: Nel[A] = Nel.of(append(offset = 0, seqNr = 1, seqNrs = 2))
  def b5: Nel[A] = Nel.of(append(offset = 0, seqNr = 1, seqNrs = 2), append(offset = 1, seqNr = 3, seqNrs = 4))
  def b6: Nel[A] = Nel.of(append(offset = 0, seqNr = 1), mark(offset = 1))
  def b7: Nel[A] = Nel.of(mark(offset = 0), append(offset = 1, seqNr = 1))
  def b8: Nel[A] = Nel.of(append(offset = 0, seqNr = 1), append(offset = 1, seqNr = 2))
  def b9: Nel[A] = Nel.of(
    mark(offset = 0),
    append(offset = 1, seqNr = 1),
    mark(offset = 2),
    append(offset = 3, seqNr = 2),
    mark(offset = 4),
  )
  def b10: Nel[A] = Nel.of(delete(offset = 1, to = 1))
  def b11: Nel[A] = Nel.of(mark(offset = 1), delete(offset = 2, to = 1))
  def b12: Nel[A] = Nel.of(delete(offset = 1, to = 1), mark(offset = 2))
  def b13: Nel[A] = Nel.of(delete(offset = 1, to = 1), append(offset = 2, seqNr = 2))
  def b14: Nel[A] = Nel.of(append(offset = 1, seqNr = 2), delete(offset = 2, to = 1))
  def b15: Nel[A] = Nel.of(append(offset = 1, seqNr = 1, seqNrs = 2, 3), delete(offset = 2, to = 1))
  def b16: Nel[A] = Nel.of(append(offset = 1, seqNr = 1), delete(offset = 2, to = 1), append(offset = 3, seqNr = 2))
  def b17: Nel[A] = Nel.of(
    append(offset = 1, seqNr = 1),
    delete(offset = 2, to = 1, origin = "origin1"),
    append(offset = 3, seqNr = 2),
    delete(offset = 4, to = 2, origin = "origin2"),
  )
  def b18: Nel[A] = Nel.of(
    append(offset = 1, seqNr = 1),
    delete(offset = 2, to = 1, origin = "origin"),
    append(offset = 3, seqNr = 2),
    delete(offset = 4, to = 2),
  )
  def b19: Nel[A] = Nel.of(
    append(offset = 1, seqNr = 1),
    append(offset = 2, seqNr = 2),
    delete(offset = 3, to = 1, origin = "origin1"),
    delete(offset = 4, to = 2, origin = "origin2"),
  )
  def b20: Nel[A] = Nel.of(
    append(offset = 1, seqNr = 1),
    append(offset = 2, seqNr = 2),
    delete(offset = 3, to = 1),
    delete(offset = 4, to = 2, origin = "origin"),
  )
  def b21: Nel[A] = Nel.of(delete(offset = 2, to = 1), delete(offset = 3, to = 2))
  def b22: Nel[A] = Nel.of(delete(offset = 2, to = 2, origin = "origin"), delete(offset = 3, to = 1))
  def b23: Nel[A] = Nel.of(
    mark(offset = 2),
    delete(offset = 3, to = 1, origin = "origin"),
    mark(offset = 4),
    delete(offset = 5, to = 2),
    mark(offset = 6),
  )
  def b24: Nel[A] = Nel.of(
    append(offset = 0, seqNr = 1),
    delete(offset = 1, to = 1),
    append(offset = 2, seqNr = 2),
    delete(offset = 3, to = 2),
    append(offset = 4, seqNr = 3),
  )
  def b25: Nel[A] = Nel.of(
    append(offset = 0, seqNr = 1),
    append(offset = 1, seqNr = 2),
    delete(offset = 2, to = 1),
    append(offset = 3, seqNr = 3),
    delete(offset = 4, to = 3),
    append(offset = 5, seqNr = 4),
  )
  def b26: Nel[A] = Nel.of(
    append(offset = 0, seqNr = 1),
    append(offset = 1, seqNr = 2),
    mark(offset = 2),
    delete(offset = 3, to = 1),
    append(offset = 4, seqNr = 3),
    append(offset = 5, seqNr = 4),
    mark(offset = 6),
  )
  def b27: Nel[A] = Nel.of(
    append(offset = 0, seqNr = 1),
    append(offset = 1, seqNr = 2),
    append(offset = 2, seqNr = 3),
    delete(offset = 3, to = 1, origin = "origin"),
    append(offset = 4, seqNr = 4),
    append(offset = 5, seqNr = 5),
    delete(offset = 6, to = 2),
    append(offset = 7, seqNr = 6),
  )
  def b28: Nel[A] = Nel.of(
    append(offset = 0, seqNr = 1, seqNrs = 2),
    append(offset = 1, seqNr = 3, seqNrs = 4),
    append(offset = 2, seqNr = 5),
    delete(offset = 3, to = 1),
    append(offset = 4, seqNr = 6),
    append(offset = 5, seqNr = 7),
    delete(offset = 6, to = 3),
    append(offset = 7, seqNr = 8),
  )
  def b29: Nel[A] = Nel.of(purge(offset = 0))
  def b30: Nel[A] = Nel.of(mark(offset = 0), purge(offset = 1))
  def b31: Nel[A] = Nel.of(purge(offset = 0), mark(offset = 1))
  def b32: Nel[A] = Nel.of(purge(offset = 0, origin = "origin"), mark(offset = 1), purge(offset = 2))
  def b33: Nel[A] =
    Nel.of(purge(offset = 0, origin = "origin0"), mark(offset = 1), purge(offset = 2, origin = "origin"))
  def b34: Nel[A] = Nel.of(append(offset = 0, seqNr = 1), purge(offset = 1))
  def b35: Nel[A] = Nel.of(purge(offset = 0), append(offset = 1, seqNr = 1))
  def b36: Nel[A] = Nel.of(delete(offset = 0, to = 1), purge(offset = 1))
  def b37: Nel[A] = Nel.of(purge(offset = 0), delete(offset = 1, to = 1))
  def b38: Nel[A] = Nel.of(delete(offset = 0, to = 1), delete(offset = 1, to = 2))
  def b39: Nel[A] = Nel.of(
    append(offset = 0, seqNr = 1, seqNrs = 2),
    append(offset = 1, seqNr = 3, seqNrs = 4),
    append(offset = 2, seqNr = 5, seqNrs = 6),
    delete(offset = 3, to = 3),
    delete(offset = 4, to = 5),
  )
  def b40: Nel[A] = Nel.of(
    append(offset = 0, seqNr = 1, seqNrs = 2, 3, 4, 5, 6),
    delete(offset = 1, to = 3),
    delete(offset = 2, to = 6),
  )
  def b41: Nel[A] = Nel.of(
    append(offset = 0, seqNr = 1, seqNrs = 2, 3, 4),
    append(offset = 1, seqNr = 5, seqNrs = 6),
    delete(offset = 2, to = 3),
    delete(offset = 3, to = 6),
  )
  def b42: Nel[A] = Nel.of(
    append(offset = 0, seqNr = 1, seqNrs = 2, 3, 4),
    append(offset = 1, seqNr = 5, seqNrs = 6),
    delete(offset = 2, to = 3),
  )
  def b43: Nel[A] = Nel.of(
    delete(offset = 1, to = 10),
    delete(offset = 2, to = 6),
  )
  def b44: Nel[A] = Nel.of( // state: delete_to = 384, seqNr = 573 (in Cassandra)
    append(offset = 1797039, seqNr = 574),
    append(offset = 1801629, seqNr = 575),
    mark(offset = 1801632),
    delete(offset = 1801642, to = 575),
  )

  def b45: Nel[A] = Nel.of( // state: delete_to = 384, seqNr = 573 (in Cassandra)
    append(offset = 1, seqNr = 1),
    append(offset = 2, seqNr = 2),
    append(offset = 3, seqNr = 3),
    append(offset = 4, seqNr = 4),
    append(offset = 5, seqNr = 5),
    append(offset = 6, seqNr = 6),
    append(offset = 7, seqNr = 7),
    append(offset = 8, seqNr = 8),
    append(offset = 9, seqNr = 9),
    delete(offset = 10, to = 5),
    append(offset = 11, seqNr = 11),
    append(offset = 12, seqNr = 12),
    append(offset = 13, seqNr = 13),
    append(offset = 14, seqNr = 14),
    append(offset = 15, seqNr = 15),
    append(offset = 16, seqNr = 16),
    append(offset = 17, seqNr = 17),
    append(offset = 18, seqNr = 18),
    append(offset = 19, seqNr = 19),
    delete(offset = 20, to = 15),
    append(offset = 21, seqNr = 21),
    append(offset = 22, seqNr = 22),
    append(offset = 23, seqNr = 23),
    append(offset = 24, seqNr = 24),
    append(offset = 25, seqNr = 25),
    append(offset = 26, seqNr = 26),
    append(offset = 27, seqNr = 27),
    append(offset = 28, seqNr = 28),
    append(offset = 29, seqNr = 29),
  )

  private val keyOf = Key(id = "id", topic = "topic")

  private val timestamp = Instant.now()

  def append(offset: Int, seqNr: Int, seqNrs: Int*): A.Append = {
    A.Append(offset = offset, seqNr = seqNr, seqNrs = seqNrs.toList)
  }

  def delete(offset: Int, to: Int, origin: String = ""): A = {
    A.Delete(offset = offset, seqNr = to, origin = origin)
  }

  def mark(offset: Int): A = {
    A.Mark(offset = offset)
  }

  def purge(offset: Int, origin: String = ""): A = {
    A.Purge(offset = offset, origin = origin)
  }

  def seqNrOf(value: Int): SeqNr = SeqNr.unsafe(value)

  def originOf(origin: String): Option[Origin] = {
    if (origin.isEmpty) none else Origin(origin).some
  }

  def appendOf(seqNrs: Nel[Int]): Action.Append = {
    Action.Append(
      key = keyOf,
      timestamp = timestamp,
      header = ActionHeader.Append(
        range = SeqRange(seqNrOf(seqNrs.head), seqNrOf(seqNrs.last)),
        payloadType = PayloadType.Binary,
        origin = none,
        version = none,
        metadata = HeaderMetadata.empty,
      ),
      payload = ByteVector.empty,
      headers = Headers.empty,
    )
  }

  def deleteOf(seqNr: Int, origin: String): Action.Delete = {
    Action.Delete(keyOf, timestamp, seqNrOf(seqNr).toDeleteTo, originOf(origin), version = none)
  }

  def actionOf(a: A): Action = {
    a match {
      case a: A.Append => appendOf(Nel(a.seqNr, a.seqNrs))
      case a: A.Delete => deleteOf(seqNr = a.seqNr, origin = a.origin)
      case a: A.Purge => Action.Purge(keyOf, timestamp, origin = originOf(a.origin), version = none)
      case _: A.Mark => Action.Mark(keyOf, timestamp, ActionHeader.Mark("id", none, version = none))
    }
  }

  def actionRecordOf(a: A): ActionRecord[Action] = {
    val action = actionOf(a)
    actionRecordOf(action, a.offset)
  }

  def actionRecordOf[T <: Action](action: T, offset: Int): ActionRecord[T] = {
    ActionRecord(action, PartitionOffset(offset = Offset.unsafe(offset)))
  }

  sealed trait A {
    def offset: Int
  }

  object A {

    case class Append(offset: Int, seqNr: Int, seqNrs: List[Int]) extends A {
      override def toString: String = {
        val range = seqNrs.lastOption.fold(seqNr.toString) { to => s"$seqNr..$to" }
        s"$productPrefix($offset,$range)"
      }
    }

    case class Delete(offset: Int, seqNr: Int, origin: String) extends A

    case class Mark(offset: Int) extends A

    case class Purge(offset: Int, origin: String) extends A
  }
}
