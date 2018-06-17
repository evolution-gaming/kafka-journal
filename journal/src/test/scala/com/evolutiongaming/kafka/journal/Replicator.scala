package com.evolutiongaming.kafka.journal

import java.util.regex.Pattern

import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, CreateConsumer}

import scala.concurrent.duration._

object Replicator extends App {

  val commonConfig = CommonConfig(clientId = Some("replicator"))

  val config = ConsumerConfig(
    common = commonConfig,
    autoOffsetReset = AutoOffsetReset.Earliest,
    enableAutoCommit = false)

  val consumer = CreateConsumer[String, Array[Byte]](config)
  val pattern = Pattern.compile(".*")
  //  consumer.subscribe(pattern)

  val topic = "test"
  val topics = List(topic)
  consumer.subscribe(topics)

  while (true) {
    val consumerRecords = consumer.poll(1.second)
    for {
      (_, records) <- consumerRecords.values
      record <- records
    } {
      println(s"record $record")
    }
  }
}