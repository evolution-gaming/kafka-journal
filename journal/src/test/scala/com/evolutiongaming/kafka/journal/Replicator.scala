package com.evolutiongaming.kafka.journal

import java.util.regex.Pattern
import java.util.{Properties, UUID}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._

object Replicator extends App {


  //17:22:52.563 DEBUG o.a.k.c.c.i.AbstractCoordinator - [Consumer clientId=consumer-1, groupId=test] Received FindCoordinator response ClientResponse(receivedTimeMs=1528384972563, latencyMs=1, disconnected=false, requestHeader=RequestHeader(apiKey=FIND_COORDINATOR, apiVersion=1, clientId=consumer-1, correlationId=11046), responseBody=FindCoordinatorResponse(throttleTimeMs=0, errorMessage='null', error=COORDINATOR_NOT_AVAILABLE, node=:-1 (id: -1 rack: null)))

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  val groupId = UUID.randomUUID().toString
  props.put("group.id", "Replicator")
  props.put("enable.auto.commit", "false")
  props.put("auto.offset.reset", "earliest")
  //  props.put("zookeeper.connect", "localhost:2181");
  //  props.put("auto.commit.interval.ms", "1000")
  //  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  //  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")


  val deserializer = new StringDeserializer()

  val consumer = com.evolutiongaming.skafka.consumer.Consumer(new KafkaConsumer[String, String](props, deserializer, deserializer))
  val pattern = Pattern.compile(".*")
  //  consumer.subscribe(pattern)

  val topic = "test"
  val topics = List(topic)
  //  consumer.assign(partitions)
  consumer.subscribe(topics)
  //  consumer.assign(partitions)
  //  consumer.seekToBeginning(partitions)
  //  consumer.subscribe(topics)

  //  consumer.seekToBeginning()
  while (true) {
    val consumerRecords = consumer.poll(1.second)
    for {
      (_, records) <- consumerRecords.values
      record <- records
    } {
      println(s"record $record")
    }
    //    if(records.nonEmpty) consumer.commitSync()
  }
}