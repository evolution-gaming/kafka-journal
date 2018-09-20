package com.evolutiongaming.kafka.journal


import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.producer.ProducerRecord

object KafkaConverters {
  private val `journal.action` = "journal.action"

  implicit class ActionHeaderOps(val self: ActionHeader) extends AnyVal {

    def toHeader: skafka.Header = Header(`journal.action`, self.toBytes)
  }


  implicit class KafkaHeaderOps(val self: skafka.Header) extends AnyVal {

    def toActionHeader: ActionHeader = ActionHeader(self.value)
  }


  implicit class ActionOps(val self: Action) extends AnyVal {

    def toProducerRecord: ProducerRecord[String, Bytes] = {
      val key = self.key
      val actionHeader = ActionHeader(self)
      val header = Header(`journal.action`, actionHeader.toBytes)
      val payload = self match {
        case action: Action.Append => action.events
        case action: Action.Delete => Bytes.Empty
        case action: Action.Mark   => Bytes.Empty
      }
      ProducerRecord(
        topic = key.topic,
        value = Some(payload),
        key = Some(key.id),
        timestamp = Some(self.timestamp),
        headers = List(header))
    }
  }


  implicit class ConsumerRecordOps(val self: ConsumerRecord[String, Bytes]) extends AnyVal {

    def toAction: Option[Action] = {
      for {
        id <- self.key
        value <- self.value
        kafkaHeader <- self.headers.find { _.key == `journal.action` }
        header = kafkaHeader.toActionHeader
        timestampAndType <- self.timestampAndType
        timestamp = timestampAndType.timestamp
        key = Key(id = id.value, topic = self.topic)
      } yield header match {
        case header: ActionHeader.Append => Action.Append(key, timestamp, header.origin, header.range, value.value)
        case header: ActionHeader.Delete => Action.Delete(key, timestamp, header.origin, header.to)
        case header: ActionHeader.Mark   => Action.Mark(key, timestamp, header.origin, header.id)
      }
    }
  }
}
