package com.evolutiongaming.kafka.journal


import java.time.Instant

import cats.data.OptionT
import cats.implicits._
import cats.{ApplicativeError, MonadError}
import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.producer.ProducerRecord
import scodec.bits.ByteVector

object KafkaConversions {

  val `journal.action` = "journal.action"


  implicit def actionHeaderToHeader[F[_]](implicit
    F: ApplicativeError[F, Throwable],
    actionHeaderToBytes: ToBytes[F, ActionHeader]
  ): Conversion[F, ActionHeader, Header] = {
    a: ActionHeader => {
      val result = for {
        bytes <- actionHeaderToBytes(a)
      } yield {
        Header(`journal.action`, bytes.toArray)
      }
      result.handleErrorWith { cause =>
        JournalError(s"actionHeaderToHeader failed for $a: $cause", cause.some).raiseError[F, Header]
      }
    }
  }


  implicit def tupleToHeader[F[_]](implicit
    F: ApplicativeError[F, Throwable],
    stringToBytes: ToBytes[F, String],
  ): Conversion[F, (String, String), Header] = {
    case a @ (key, value) =>
      val result = for {
        value <- stringToBytes(value)
      } yield {
        Header(key, value.toArray)
      }
      result.handleErrorWith { cause =>
        JournalError(s"headerToSkafkaHeader failed for $a: $cause", cause.some).raiseError[F, Header]
      }
  }


  implicit def actionToProducerRecord[F[_]](implicit
    F: MonadError[F, Throwable],
    actionHeaderToHeader: Conversion[F, ActionHeader, Header],
    tupleToHeader: Conversion[F, (String, String), Header]
  ): Conversion[F, Action, ProducerRecord[Id, ByteVector]] = {

    a: Action => {
      val key = a.key
      val result = for {
        header  <- actionHeaderToHeader(a.header)
        headers <- a match {
          case a: Action.Append => a.headers.toList.traverse(tupleToHeader.apply)
          case _: Action.Delete => List.empty[Header].pure[F]
          case _: Action.Mark   => List.empty[Header].pure[F]
        }
      } yield {
        val payload = a match {
          case a: Action.Append => a.payload.some
          case _: Action.Delete => none
          case _: Action.Mark   => none
        }

        ProducerRecord(
          topic = key.topic,
          value = payload,
          key = key.id.some,
          timestamp = a.timestamp.some,
          headers = header :: headers)
      }
      result.handleErrorWith { cause =>
        JournalError(s"actionToProducerRecord failed for $a: $cause", cause.some).raiseError[F, ProducerRecord[Id, ByteVector]]
      }
    }
  }


  implicit def consumerRecordToActionHeader[F[_]](implicit
    F: ApplicativeError[F, Throwable],
    fromBytes: FromBytes[F, ActionHeader]
  ): Conversion[Option, ConsumerRecord[Id, ByteVector], F[ActionHeader]] = {
    a: ConsumerRecord[Id, ByteVector] => {
      for {
        header <- a.headers.find { _.key == `journal.action` }
      } yield {
        val byteVector = ByteVector.view(header.value)
        val actionHeader = fromBytes(byteVector)
        actionHeader.handleErrorWith { cause =>
          JournalError(s"consumerRecordToActionHeader failed for $a: $cause", cause.some).raiseError[F, ActionHeader]
        }
      }
    }
  }


  implicit def headerToTuple[F[_]](implicit
    F: ApplicativeError[F, Throwable],
    stringFromBytes: FromBytes[F, String],
  ): Conversion[F, Header, (String, String)] = {
    a: Header => {
      val bytes = ByteVector.view(a.value)
      val result = for {
        value <- stringFromBytes(bytes)
      } yield {
        (a.key, value)
      }
      result.handleErrorWith { cause =>
        JournalError(s"headerToTuple failed for $a: $cause", cause.some).raiseError[F, (String, String)]
      }
    }
  }


  implicit def consumerRecordToActionRecord[F[_]](implicit
    F: MonadError[F, Throwable],
    consumerRecordToActionHeader: Conversion[Option, ConsumerRecord[Id, ByteVector], F[ActionHeader]],
    headerToTuple: Conversion[F, Header, (String, String)],
  ): Conversion[OptionT[F, ?], ConsumerRecord[Id, ByteVector], ActionRecord[Action]] = {

    consumerRecord: ConsumerRecord[Id, ByteVector] => {

      def action(key: Key, timestamp: Instant, header: ActionHeader) = {
        
        def append(header: ActionHeader.Append) = {
          consumerRecord.value.traverse { value =>
            val headers = consumerRecord.headers
              .filter { _.key != `journal.action` }
              .traverse(headerToTuple.apply)

            for {
              headers <- headers
            } yield {
              val payload = value.value
              Action.append(key, timestamp, header, payload, headers.toMap)
            }
          }
        }

        header match {
          case header: ActionHeader.Append => OptionT(append(header))
          case header: ActionHeader.Delete => OptionT.pure[F](Action.delete(key, timestamp, header))
          case header: ActionHeader.Mark   => OptionT.pure[F](Action.mark(key, timestamp, header))
        }
      }

      val opt = for {
        id               <- consumerRecord.key
        timestampAndType <- consumerRecord.timestampAndType
        header           <- consumerRecordToActionHeader(consumerRecord)
      } yield for {
        header    <- OptionT.liftF(header)
        key        = Key(id = id.value, topic = consumerRecord.topic)
        timestamp  = timestampAndType.timestamp
        action    <- action(key, timestamp, header)
      } yield {
        val partitionOffset = PartitionOffset(consumerRecord)
        ActionRecord(action, partitionOffset)
      }

      val result = OptionT.fromOption[F](opt).flatten.value.handleErrorWith { cause =>
        JournalError(s"consumerRecordToActionRecord failed for $consumerRecord: $cause", cause.some).raiseError[F, Option[ActionRecord[Action]]]
      }
      OptionT(result)
    }
  }
}
