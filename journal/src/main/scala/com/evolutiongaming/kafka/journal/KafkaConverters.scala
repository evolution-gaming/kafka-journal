package com.evolutiongaming.kafka.journal

import java.time.Instant

import com.evolutiongaming.kafka.journal.HeaderFormats._
import com.evolutiongaming.kafka.journal.eventual.PartitionOffset
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.producer.ProducerRecord
import com.evolutiongaming.skafka.{FromBytes, Header, ToBytes, Topic}
import play.api.libs.json.Json

object KafkaConverters {
  private val `journal.action` = "journal.action"


  implicit val bytesToBytes: ToBytes[Bytes] = new ToBytes[Bytes] {
    def apply(value: Bytes, topic: Topic) = value.value
  }

  implicit val bytesFromBytes: FromBytes[Bytes] = new FromBytes[Bytes] {
    def apply(bytes: Array[Byte], topic: Topic): Bytes = Bytes(bytes)
  }


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
        value = payload,
        key = Some(key.id),
        timestamp = timestamp.map(_.toEpochMilli),
        headers = List(header))
    }
  }


  implicit class ConsumerRecordOps(val self: ConsumerRecord[String, Bytes]) extends AnyVal {

    def toPartitionOffset: PartitionOffset = {
      PartitionOffset(partition = self.partition, offset = self.offset)
    }

    def toAction: Option[Action] = {

      def action(header: Action.Header) = {

        def timestamp = {
          for {
            timestampAndType <- self.timestampAndType
          } yield {
            val timestamp = timestampAndType.timestamp
            Instant.ofEpochMilli(timestamp)
          }
        }

        def append(header: Action.Header.Append) = {
          for {
            timestamp <- timestamp
          } yield {
            Action.Append(header, timestamp, self.value)
          }
        }

        def delete(header: Action.Header.Delete) = {
          for {
            timestamp <- timestamp
          } yield {
            Action.Delete(header, timestamp)
          }
        }

        header match {
          case header: Action.Header.Append => append(header)
          case header: Action.Header.Delete => delete(header)
          case header: Action.Header.Mark   => Some(Action.Mark(header))
        }
      }

      for {
        kafkaHeader <- self.headers.find { _.key == `journal.action` }
        header = kafkaHeader.toActionHeader
        action <- action(header)
      } yield {
        action
      }
    }

    def toKafkaRecord: Option[KafkaRecord.Any] = {
      for {
        id <- self.key
        action <- self.toAction
      } yield {
        val key = Key(id, self.topic)
        KafkaRecord(key, action)
      }
    }

    // TODO rename
    def toKafkaRecord2(): Option[KafkaRecord2[_ <: Action]] = {
      for {
        kafkaRecord <- self.toKafkaRecord
      } yield {
        KafkaRecord2(kafkaRecord, self.toPartitionOffset)
      }
    }
  }
}
