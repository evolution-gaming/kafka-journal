package com.evolutiongaming.kafka.journal.cassandra

import com.evolutiongaming.kafka.journal.cassandra.PlayJsonHelperExtension.*
import com.evolutiongaming.kafka.journal.{JsonCodec, RecordMetadata}
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}

import scala.util.Try

object RecordMetadataExtension {

  implicit def encodeByNameRecordMetadata(
    implicit
    encode: JsonCodec.Encode[Try],
  ): EncodeByName[RecordMetadata] =
    encodeByNameFromWrites

  implicit def decodeByNameRecordMetadata(
    implicit
    decode: JsonCodec.Decode[Try],
  ): DecodeByName[RecordMetadata] =
    decodeByNameFromReads

  implicit def encodeRowRecordMetadata(
    implicit
    encode: JsonCodec.Encode[Try],
  ): EncodeRow[RecordMetadata] =
    EncodeRow("metadata")

  implicit def decodeRowRecordMetadata(
    implicit
    decode: JsonCodec.Decode[Try],
  ): DecodeRow[RecordMetadata] =
    DecodeRow("metadata")
}
