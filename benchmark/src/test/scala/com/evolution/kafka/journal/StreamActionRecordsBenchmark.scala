package com.evolution.kafka.journal

import cats.implicits.*
import com.evolutiongaming.skafka.{Offset, Partition}
import com.evolutiongaming.sstream.Stream
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * Exercises the sstream-based journal read pipeline ([[StreamActionRecords]] over an in-memory
 * [[ConsumeActionRecords]]) to measure the influence of the sstream dependency.
 *
 * To run: {{{sbt benchmark/Jmh/run com.evolution.kafka.journal.StreamActionRecordsBenchmark}}}
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
class StreamActionRecordsBenchmark {

  import JournalBenchmarkData.key
  import StreamActionRecordsSpec.{State, StateT, bracket}

  // scalastyle:off var.field
  @Param(Array("1000", "10000"))
  var size: Int = _

  private var records: List[ActionRecord[Action]] = _
  private var marker: Marker = _
  // scalastyle:on var.field

  @Setup
  def setup(): Unit = {
    val appends = (1 to size).toList.map { i => JournalBenchmarkData.appendRecord(i.toLong, i.toLong) }
    val markOffset = Offset.unsafe(size.toLong + 1)
    val mark = Action.Mark(key, JournalBenchmarkData.timestamp, ActionHeader.Mark("mark", none, Version.current.some))
    val markRecord = ActionRecord(mark, PartitionOffset(offset = markOffset))
    marker = Marker(mark.id, PartitionOffset(offset = markOffset))
    records = appends :+ markRecord
  }

  @Benchmark
  def readAll(blackhole: Blackhole): Unit = {
    blackhole.consume(run(from = SeqNr.min, replicated = None, offset = None))
  }

  @Benchmark
  def readTail(blackhole: Blackhole): Unit = {
    val half = Offset.unsafe(size.toLong / 2)
    blackhole.consume(run(from = SeqNr.min, replicated = half.some, offset = None))
  }

  private def run(from: SeqNr, replicated: Option[Offset], offset: Option[Offset]): List[ActionRecord[Action.User]] = {
    val consume: ConsumeActionRecords[StateT] = { (_: Key, _: Partition, from: Offset) =>
      val next = StateT { state =>
        val remaining = state.records.dropWhile { _.offset < from }
        remaining match {
          case h :: t => (state.copy(records = t), Stream[StateT].single(h))
          case Nil => (state, Stream[StateT].empty[ActionRecord[Action]])
        }
      }
      Stream.repeat(next).flatten
    }
    val stream = StreamActionRecords[StateT](key, from, marker, replicated, consume)
    stream(offset).toList.run(State(records)).get._2
  }
}
