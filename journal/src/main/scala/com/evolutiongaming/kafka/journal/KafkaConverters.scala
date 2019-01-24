package com.evolutiongaming.kafka.journal


import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.producer.ProducerRecord

object KafkaConverters {

  private val `journal.action` = "journal.action"


  implicit class ActionHeaderOps(val self: ActionHeader) extends AnyVal {

    def toHeader: Header = Header(`journal.action`, self.toBytes)
  }


  implicit class ActionOps(val self: Action) extends AnyVal {

    def toProducerRecord: ProducerRecord[Id, Bytes] = {
      val key = self.key
      val actionHeader = ActionHeader(self)
      val header = Header(`journal.action`, actionHeader.toBytes)
      val payload = self match {
        case a: Action.Append => Some(a.payload.value)
        case _: Action.Delete => None
        case _: Action.Mark   => None
      }
      ProducerRecord(
        topic = key.topic,
        value = payload,
        key = Some(key.id),
        timestamp = Some(self.timestamp),
        headers = List(header))
    }
  }


  implicit class ConsumerRecordOps(val self: ConsumerRecord[Id, Bytes]) extends AnyVal {

    def toActionHeader: Option[ActionHeader] = {
      for {
        headerKafka <- self.headers.find { _.key == `journal.action` }
        header       = headerKafka.value.fromBytes[ActionHeader]
      } yield header
    }

    def toAction: Option[Action] = {
      for {
        id               <- self.key
        header           <- self.toActionHeader
        timestampAndType <- self.timestampAndType
        timestamp         = timestampAndType.timestamp
        key               = Key(id = id.value, topic = self.topic)
        origin            = header.origin
        action           <- header match {
          case header: ActionHeader.Append =>
            for {
              value <- self.value
            } yield {
              val payload = Payload.Binary(value.value)
              Action.Append(key, timestamp, origin, header.range, header.payloadType, payload)
            }
          case header: ActionHeader.Delete => Some(Action.Delete(key, timestamp, origin, header.to))
          case header: ActionHeader.Mark   => Some(Action.Mark(key, timestamp, origin, header.id))
        }
      } yield action
    }
  }
}
