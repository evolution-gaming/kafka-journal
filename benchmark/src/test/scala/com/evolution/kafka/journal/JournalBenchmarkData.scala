package com.evolution.kafka.journal

import cats.syntax.all.*
import com.evolutiongaming.skafka.Offset
import scodec.bits.ByteVector

import java.time.Instant

object JournalBenchmarkData {

  val key: Key = Key(topic = "topic", id = "id")

  val timestamp: Instant = Instant.now()

  def appendRecord(seqNr: Long, offset: Long): ActionRecord[Action] = {
    val header = ActionHeader.Append(
      range = SeqRange.unsafe(seqNr),
      origin = none,
      version = Version.current.some,
      payloadType = PayloadType.Json,
      metadata = HeaderMetadata.empty,
    )
    val action = Action.Append(key, timestamp, header, ByteVector.empty, Headers.empty)
    ActionRecord(action, PartitionOffset(offset = Offset.unsafe(offset)))
  }
}
