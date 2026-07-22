package com.evolution.kafka.journal

import cats.data.NonEmptyList as Nel
import cats.data.NonEmptySet as Nes
import cats.effect.{Resource, SyncIO}
import cats.syntax.all.*
import com.evolution.kafka.journal.conversions.ConsRecordToActionRecord
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords, WithSize}
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * Drives the production [[ConsumeActionRecords]] poll loop over a fake [[Journals.Consumer]] that
 * replays a fixed poll batch, with a stub [[ConsRecordToActionRecord]] so the measured cost is the
 * per-batch collection plumbing (values/filter/traverseFilter), not payload decoding.
 *
 * To run: {{{sbt benchmark/Jmh/run com.evolution.kafka.journal.ConsumeActionRecordsBenchmark}}}
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
class ConsumeActionRecordsBenchmark {

  import ConsumeActionRecordsBenchmark.*
  import JournalBenchmarkData.key

  // scalastyle:off var.field
  @Param(Array("1000", "10000"))
  var size: Int = _

  private val batchSize = 100

  private var consume: ConsumeActionRecords[SyncIO] = _
  // scalastyle:on var.field

  @Setup
  def setup(): Unit = {
    val batch = ConsumerRecords(Map(topicPartition -> Nel.fromListUnsafe(List.fill(batchSize)(consRecord))))
    val consumer = new Journals.Consumer[SyncIO] {
      def assign(partitions: Nes[TopicPartition]): SyncIO[Unit] = SyncIO.unit
      def seek(partition: TopicPartition, offset: Offset): SyncIO[Unit] = SyncIO.unit
      def poll: SyncIO[ConsRecords] = SyncIO.pure(batch)
    }
    consume = ConsumeActionRecords[SyncIO](Resource.pure(consumer))
  }

  @Benchmark
  def poll(blackhole: Blackhole): Unit = {
    val records = consume(key, Partition.min, Offset.min)
      .take(size.toLong)
      .toList
      .unsafeRunSync()
    blackhole.consume(records)
  }
}

object ConsumeActionRecordsBenchmark {

  import JournalBenchmarkData.key

  private val topicPartition = TopicPartition(topic = key.topic, partition = Partition.min)

  private val consRecord: ConsRecord = ConsumerRecord(
    topicPartition = topicPartition,
    offset = Offset.min,
    timestampAndType = none,
    key = WithSize(key.id).some,
    value = none,
    headers = Nil,
  )

  private val actionRecord: ActionRecord[Action] = JournalBenchmarkData.appendRecord(seqNr = 1L, offset = 0L)

  private implicit val consRecordToActionRecord: ConsRecordToActionRecord[SyncIO] =
    (_: ConsRecord) => SyncIO.pure(actionRecord.some)
}
