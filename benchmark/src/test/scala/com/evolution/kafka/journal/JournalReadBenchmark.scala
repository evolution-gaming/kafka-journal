package com.evolution.kafka.journal

import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.evolution.kafka.journal.JournalSpec.*
import com.evolution.kafka.journal.eventual.EventualJournal
import org.openjdk.jmh.annotations.{Benchmark, Fork, Level, Measurement, Scope, Setup, State as JmhState, Warmup}
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * Performance coverage of the worst-case journal-read scenarios in [[JournalReadWorstCaseSpec]]
 * (issue #14). Materialises `Journals.read` over the in-memory model reused from [[JournalSpec]];
 * the state per shape is built once in [[setup]] and the measured method only runs the read.
 * `replicatedFastPath` is the baseline; the others add a cold cache, a large tail, a merge seam or a
 * duplicated tail on top.
 *
 * {{{sbt benchmark/Jmh/run com.evolution.kafka.journal.JournalReadBenchmark}}}
 */
@JmhState(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
class JournalReadBenchmark {

  import JournalReadBenchmark.*

  private val size = 10000

  private var replicatedFastPathShape: Shape = _
  private var coldCacheLargeTailShape: Shape = _
  private var warmCacheLargeTailShape: Shape = _
  private var mergeSeamShape: Shape = _
  private var duplicatedTailShape: Shape = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val defaultState = stateOf(StateT.produceAction, size)

    replicatedFastPathShape = Shape(
      journal = SeqNrJournal(StateT.eventualJournal, StateT.consumeActionRecords, StateT.produceAction, StateT.headCache),
      state = defaultState,
    )

    coldCacheLargeTailShape = Shape(
      journal = SeqNrJournal(
        EventualJournal.empty[StateT],
        StateT.consumeActionRecords,
        StateT.produceAction,
        HeadCache.const(none[HeadInfo].pure[StateT]),
      ),
      state = defaultState,
    )

    warmCacheLargeTailShape = Shape(
      journal =
        SeqNrJournal(EventualJournal.empty[StateT], StateT.consumeActionRecords, StateT.produceAction, StateT.headCache),
      state = defaultState,
    )

    val behind = StateT.eventualActionsBehind(size / 2)
    mergeSeamShape = Shape(
      journal = SeqNrJournal(StateT.eventualJournal, StateT.consumeActionRecords, behind, StateT.headCache),
      state = stateOf(behind, size),
    )

    duplicatedTailShape = Shape(
      journal = SeqNrJournal(
        EventualJournal.empty[StateT],
        StateT.consumeActionRecords.withDuplicates,
        StateT.produceAction,
        StateT.headCache,
      ),
      state = defaultState,
    )
  }

  @Benchmark
  def replicatedFastPath(blackhole: Blackhole): Unit = readAll(blackhole, replicatedFastPathShape)

  @Benchmark
  def coldCacheLargeTail(blackhole: Blackhole): Unit = readAll(blackhole, coldCacheLargeTailShape)

  @Benchmark
  def warmCacheLargeTail(blackhole: Blackhole): Unit = readAll(blackhole, warmCacheLargeTailShape)

  @Benchmark
  def mergeSeam(blackhole: Blackhole): Unit = readAll(blackhole, mergeSeamShape)

  @Benchmark
  def duplicatedTail(blackhole: Blackhole): Unit = readAll(blackhole, duplicatedTailShape)

  private def readAll(blackhole: Blackhole, shape: Shape): Unit = {
    val seqNrs = shape
      .journal
      .read(SeqRange.all)
      .run(shape.state)
      .map { case (_, a) => a }
      .unsafeRunSync()
    blackhole.consume(seqNrs)
  }
}

object JournalReadBenchmark {

  private final case class Shape(journal: SeqNrJournal[StateT], state: State)

  private def stateOf(produceAction: ProduceAction[StateT], size: Int): State = {
    val journal =
      SeqNrJournal(StateT.eventualJournal, StateT.consumeActionRecords, produceAction, StateT.headCache)
    (1 to size)
      .toList
      .foldLeftM(()) { (_, n) => journal.append(SeqNr.unsafe(n)).void }
      .run(State.empty)
      .map { case (state, _) => state }
      .unsafeRunSync()
  }
}
