package com.evolution.kafka.journal

import org.openjdk.jmh.annotations.{Benchmark, Fork, Measurement, Scope, State as JmhState, Warmup}
import play.api.libs.json.Json
import scodec.bits.ByteVector

import java.util.concurrent.TimeUnit
import scala.util.Try

/**
 * Decoding of the `journal.action` header in the head cache.
 *
 * `full` is the full decoder (JSON to `JsValue`, then `Reads`), which [[HeadAction.fromJson]] falls
 * back to. `fast` is [[HeadAction.fromJson]].
 *
 * Run with `runBenchmarks -prof gc HeadActionBenchmark` to also see the allocation.
 */
@JmhState(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
class HeadActionBenchmark {

  private implicit val jsonCodec: JsonCodec[Try] = JsonCodec.default[Try]

  private val fromBytes: FromBytes[Try, Option[ActionHeader]] = ActionHeader.fromBytesOptActionHeader[Try]

  // a typical append header, with origin, version and metadata
  private val header: Array[Byte] = Json.toBytes(
    Json.toJson[ActionHeader](
      ActionHeader.Append(
        range = SeqRange.unsafe(1, 3),
        origin = Some(Origin("live-game-5dc6d7fb4b-57k9l")),
        version = Some(Version("11.2.0")),
        payloadType = PayloadType.Json,
        metadata = HeaderMetadata(Some(Json.obj("key" -> "value", "time" -> 1795678947516L))),
      ),
    ),
  )

  @Benchmark
  def full(): Option[HeadAction] = fromBytes(ByteVector.view(header)).get.map(HeadAction(_))

  @Benchmark
  def fast(): Option[Option[HeadAction]] = HeadAction.fromJson(header)
}
