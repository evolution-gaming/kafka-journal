package com.evolution.kafka

import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords}
import scodec.bits.ByteVector

package object journal {

  type Tag = String

  type Tags = Set[Tag]

  type Headers = Map[String, String]

  type ConsRecord = ConsumerRecord[String, ByteVector]

  type ConsRecords = ConsumerRecords[String, ByteVector]
}
