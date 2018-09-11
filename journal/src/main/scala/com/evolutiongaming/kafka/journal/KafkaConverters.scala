package com.evolutiongaming.kafka.journal


import com.evolutiongaming.kafka.journal.HeaderFormats._
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.producer.ProducerRecord
import com.evolutiongaming.skafka.Header
import play.api.libs.json.Json

object KafkaConverters {
  private val `journal.action` = "journal.action"

  implicit class ActionHeaderOps(val self: Action.Header) extends AnyVal {

    def toKafkaHeader: skafka.Header = {
      val json = Json.toJson(self)
      val bytes = Json.toBytes(json)
      Header(`journal.action`, bytes)
    }
  }


  implicit class KafkaHeaderOps(val self: skafka.Header) extends AnyVal {

    def toActionHeader: Action.Header = {
      val bytes = self.value
      val json = Json.parse(bytes)
      json.as[Action.Header]
    }
  }


  implicit class KafkaRecordOps(val self: KafkaRecord.Any) extends AnyVal {

    def toProducerRecord: ProducerRecord[String, Bytes] = {
      val action = self.action
      val key = self.key
      val header = action.header.toKafkaHeader
      val (payload, timestamp) = action match {
        case action: Action.Append => (action.events, Some(action.timestamp))
        case action: Action.Delete => (Bytes.Empty, Some(action.timestamp))
        case action: Action.Mark   => (Bytes.Empty, None)
      }
      ProducerRecord(
        topic = key.topic,
        value = Some(payload),
        key = Some(key.id),
        timestamp = timestamp,
        headers = List(header))
    }
  }


  implicit class ConsumerRecordOps(val self: ConsumerRecord[String, Bytes]) extends AnyVal {

    def toPartitionOffset: PartitionOffset = {
      PartitionOffset(partition = self.partition, offset = self.offset)
    }

    def toAction: Option[Action] = {
      for {
        withSize <- self.value
        value = withSize.value
        kafkaHeader <- self.headers.find { _.key == `journal.action` }
        header = kafkaHeader.toActionHeader
        timestampAndType <- self.timestampAndType
        timestamp = timestampAndType.timestamp
      } yield {
        header match {
          case header: Action.Header.Append => Action.Append(header, timestamp, value)
          case header: Action.Header.Delete => Action.Delete(header, timestamp)
          case header: Action.Header.Mark   => Action.Mark(header, timestamp)
        }
      }
    }

    def toKafkaRecord: Option[KafkaRecord.Any] = {
      for {
        id <- self.key
        action <- self.toAction
      } yield {
        val key = Key(topic = self.topic, id = id.value)
        KafkaRecord(key, action)
      }
    }
  }
}
