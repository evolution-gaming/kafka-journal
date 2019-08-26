package com.evolutiongaming.kafka.journal


import java.time.Instant

import cats.data.OptionT
import cats.implicits._
import cats.{Functor, Monad}
import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.producer.ProducerRecord
import scodec.bits.ByteVector

object KafkaConversions {

  val `journal.action` = "journal.action"


  implicit def actionHeaderToHeader[F[_] : Functor](implicit
    actionHeaderToBytes: ToBytes[F, ActionHeader]
  ): Conversion[F, ActionHeader, Header] = {
    a: ActionHeader => {
      for {
        bytes <- actionHeaderToBytes(a)
      } yield {
        Header(`journal.action`, bytes.toArray)
      }
    }
  }


  implicit def actionToProducerRecord[F[_] : Monad](implicit
    actionHeaderToHeader: Conversion[F, ActionHeader, Header],
    stringToBytes: ToBytes[F, String],
  ): Conversion[F, Action, ProducerRecord[Id, ByteVector]] = {

    a: Action => {
      val key = a.key

      def headers(a: Action.Append) = {
        a.headers.toList.traverse { case (key, value) =>
          for {
            bytes <- stringToBytes(value)
          } yield {
            Header(key, bytes.toArray)
          }
        }
      }

      for {
        header  <- actionHeaderToHeader(a.header)
        headers <- a match {
          case a: Action.Append => headers(a)
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
    }
  }


  implicit def consumerRecordToActionHeader[F[_]](implicit
    fromBytes: FromBytes[F, ActionHeader]
  ): Conversion[Option, ConsumerRecord[Id, ByteVector], F[ActionHeader]] = {
    a: ConsumerRecord[Id, ByteVector] => {
      for {
        header <- a.headers.find { _.key == `journal.action` }
      } yield {
        val byteVector = ByteVector.view(header.value)
        fromBytes(byteVector)
      }
    }
  }


  implicit def consumerRecordToActionRecord[F[_] : Monad](implicit
    consumerRecordToActionHeader: Conversion[Option, ConsumerRecord[Id, ByteVector], F[ActionHeader]],
    stringFromBytes: FromBytes[F, String],
  ): Conversion[OptionT[F, ?], ConsumerRecord[Id, ByteVector], ActionRecord[Action]] = {

    self: ConsumerRecord[Id, ByteVector] => {

      def action(
        key: Key,
        timestamp: Instant,
        header: ActionHeader
      ) = {
        def append(header: ActionHeader.Append) = {
          self.value.traverse { value =>
            val headers = self.headers
              .filter { _.key != `journal.action` }
              .traverse { header =>
                val bytes = ByteVector.view(header.value)
                for {
                  value <- stringFromBytes(bytes)
                } yield {
                  (header.key, value)
                }
              }

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

      val result = for {
        id               <- self.key
        timestampAndType <- self.timestampAndType
        header           <- consumerRecordToActionHeader(self)
      } yield for {
        header    <- OptionT.liftF(header)
        key        = Key(id = id.value, topic = self.topic)
        timestamp  = timestampAndType.timestamp
        action    <- action(key, timestamp, header)
      } yield {
        val partitionOffset = PartitionOffset(self)
        ActionRecord(action, partitionOffset)
      }

      OptionT.fromOption[F](result).flatten
    }
  }
}
