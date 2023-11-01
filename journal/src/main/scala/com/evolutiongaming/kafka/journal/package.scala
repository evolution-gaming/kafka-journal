package com.evolutiongaming.kafka

import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords}
import scodec.bits.ByteVector

package object journal {

  type Tag = String

  type Tags = Set[Tag]


  type Headers = Map[String, String]


  type ConsRecord = ConsumerRecord[String, ByteVector]

  type ConsRecords = ConsumerRecords[String, ByteVector]

  val rbowTopic    = "rbow-journal-4.PlayerBetting"
  val pointerTopic = "kafka-journal-offset-temp"
}
