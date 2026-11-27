package com.evolution.kafka.journal

import cats.syntax.all.*
import com.evolution.kafka.journal.conversions.ConsRecordToActionHeader
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, WithSize}
import com.evolutiongaming.skafka.{Header, Offset, Partition, TimestampAndType, TimestampType, TopicPartition}
import org.openjdk.jmh.annotations.{
  Benchmark,
  BenchmarkMode,
  Fork,
  Measurement,
  Mode,
  OutputTimeUnit,
  Param,
  Scope,
  Setup,
  State as JmhState,
  Warmup,
}

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.util.{Success, Try}

/**
 * CPU time and allocations of parsing the [[ActionHeader]] of a consumed record with
 * [[ConsRecordToActionHeader]], as [[TopicCache]] does for every record, using `Try` as the effect.
 *
 * Run with the GC profiler, `gc.alloc.rate.norm` is the number of bytes allocated per call:
 * {{{sbt "benchmark/Jmh/run -prof gc com.evolution.kafka.journal.ConsRecordToActionHeaderBenchmark"}}}
 */
@JmhState(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2, jvmArgsAppend = Array("-Xms1g", "-Xmx1g"))
class ConsRecordToActionHeaderBenchmark {
  import ConsRecordToActionHeaderBenchmark.*

  /**
   * [[JsonCodec]], a key of [[ConsRecordToActionHeaderBenchmark.codecs]].
   */
  @Param(Array("default", "streaming"))
  var codec: String = _

  /**
   * The kind of action, a key of [[ConsRecordToActionHeaderBenchmark.actionHeaders]].
   */
  @Param(Array("append", "mark", "delete", "purge"))
  var action: String = _

  private var consRecordToActionHeader: ConsRecordToActionHeader[Try] = _
  private var record: ConsumerRecord[String, Unit] = _

  @Setup
  def setup(): Unit = {
    implicit val jsonCodec: JsonCodec[Try] = codecs(codec)
    consRecordToActionHeader = ConsRecordToActionHeader[Try]
    record = recordOf(actionHeaders(action))
    val result = apply()
    require(result == Success(actionHeaders(action).some), s"unexpected result: $result")
  }

  @Benchmark
  def apply(): Try[Option[ActionHeader]] = consRecordToActionHeader(record).value
}

object ConsRecordToActionHeaderBenchmark {

  val codecs: Map[String, JsonCodec[Try]] = Map(
    "default" -> JsonCodec.default[Try],
    "streaming" -> JsonCodec.streaming[Try],
  )

  private val origin = Origin("blackjack-7d9f8b6c5-x2k4p").some
  private val version = Version("11.3.0").some

  val actionHeaders: Map[String, ActionHeader] = Map(
    "append" -> ActionHeader.Append(
      range = SeqRange(SeqNr.unsafe(4567)),
      origin = origin,
      version = version,
      payloadType = PayloadType.Json,
      metadata = HeaderMetadata.empty,
    ),
    "mark" -> ActionHeader.Mark(id = "9f5c8c1e-3c1b-4b5e-8c1a-2f6d3e4b5a6c", origin = origin, version = version),
    "delete" -> ActionHeader.Delete(to = DeleteTo(SeqNr.unsafe(4000)), origin = origin, version = version),
    "purge" -> ActionHeader.Purge(origin = origin, version = version),
  )

  /**
   * A record as produced by the journal: the action header comes first.
   */
  def recordOf(actionHeader: ActionHeader): ConsumerRecord[String, Unit] = {
    implicit val jsonCodec: JsonCodec[Try] = JsonCodec.default[Try]
    val bytes = ToBytes[Try, ActionHeader].apply(actionHeader).get
    ConsumerRecord(
      topicPartition = TopicPartition(topic = "journal.pig.blackjack", partition = Partition.unsafe(17)),
      offset = Offset.unsafe(123456789L),
      timestampAndType = TimestampAndType(Instant.now(), TimestampType.Create).some,
      key = WithSize("blackjack-table-pdbm6rrxnwiszl7f").some,
      value = WithSize(()).some,
      headers = List(Header(ActionHeader.key, bytes.toArray)),
    )
  }
}
