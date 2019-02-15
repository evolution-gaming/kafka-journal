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

    // TODO test
    def toProducerRecord: ProducerRecord[Id, Bytes] = {
      val key = self.key
      val actionHeader = self.header
      val header = Header(`journal.action`, actionHeader.toBytes)
      val payload = self match {
        case a: Action.Append => Some(a.payload.value)
        case _: Action.Delete => None
        case _: Action.Mark   => None
      }

      val headers = self match {
        case a: Action.Append => for {
          (key, value) <- a.headers.toList
        } yield {
          Header(key, value.toBytes)
        }
        case _: Action.Delete => List.empty[Header]
        case _: Action.Mark   => List.empty[Header]
      }

      ProducerRecord(
        topic = key.topic,
        value = payload,
        key = Some(key.id),
        timestamp = Some(self.timestamp),
        headers = header :: headers)
    }
  }


  implicit class ConsumerRecordOps(val self: ConsumerRecord[Id, Bytes]) extends AnyVal {

    def toActionHeader: Option[ActionHeader] = {
      for {
        headerKafka <- self.headers.find { _.key == `journal.action` }
        header       = headerKafka.value.fromBytes[ActionHeader]
      } yield header
    }

    // TODO test
    def toAction: Option[Action] = {
      for {
        id               <- self.key
        header           <- self.toActionHeader
        timestampAndType <- self.timestampAndType
        timestamp         = timestampAndType.timestamp
        key               = Key(id = id.value, topic = self.topic)
        action           <- header match {
          case header: ActionHeader.Append =>
            for {
              value <- self.value
            } yield {
              val headers = for {
                header        <- self.headers
                if header.key != `journal.action`
              } yield {
                val value = header.value.fromBytes[String]
                (header.key, value)
              }
              val payload = Payload.Binary(value.value)
              Action.Append(key, timestamp, header, payload, headers.toMap)
            }
          case header: ActionHeader.Delete => Some(Action.Delete(key, timestamp, header))
          case header: ActionHeader.Mark   => Some(Action.Mark(key, timestamp, header))
        }
      } yield action
    }

    def toActionRecord: Option[ActionRecord[Action]] = {
      for {
        action <- self.toAction
      } yield {
        val partitionOffset = PartitionOffset(self)
        ActionRecord(action, partitionOffset)
      }
    }
  }
}
