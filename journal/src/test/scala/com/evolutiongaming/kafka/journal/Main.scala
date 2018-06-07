package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.producer.Producer.Record
import com.evolutiongaming.skafka.producer.{Configs, CreateProducer, ToBytes}

import scala.compat.Platform
import scala.concurrent.Await
import scala.concurrent.duration._

object Main extends App {

  implicit val system = ActorSystem()

  val ecBlocking = system.dispatchers.lookup("kafka-plugin-blocking-dispatcher")
  implicit val ec = system.dispatcher


  val configs = Configs.Default.copy(bootstrapServers = Nel("localhost:9092"), clientId = Some("journal"))
  val producer = CreateProducer(configs, ecBlocking)


  val persistenceId = "persistenceId"
  val event = "event"
  val topic = "test"
  val timestamp = Platform.currentTime

  val toBytes = ToBytes.StringToBytes

  val record = Record(topic, event, Some(persistenceId), timestamp = Some(timestamp))


  val records = List.fill(100)(record)


  for {
    _ <- 0 to 100
  } {
    Thread.sleep(100)
    val result = producer(record)
    val metadata = Await.result(result, 5.seconds)
    println(s"metadata: $metadata")
  }

  system.terminate()
}
