package com.evolutiongaming.kafka.journal


import java.time.Instant

import cats.data.OptionT
import cats.implicits._
import cats.{FlatMap, Monad}
import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.producer.ProducerRecord
import scodec.bits.ByteVector

object KafkaConversions {

  val `journal.action` = "journal.action"

  
  implicit def actionHeaderToHeader[F[_] : FlatMap : FromTry/*TODO*/]: Conversion[F, ActionHeader, Header] = {
    a: ActionHeader => {
      for {
        bytes <- FromTry[F].unsafe { a.toBytes }
      } yield {
        Header(`journal.action`, bytes)
      }
    }
  }


  implicit def actionToProducerRecord[F[_] : Monad : FromTry/*TODO*/](implicit
    actionHeaderToHeader: Conversion[F, ActionHeader, Header]
  ): Conversion[F, Action, ProducerRecord[Id, ByteVector]] = {

    a: Action => {
      val key = a.key

      def headers(a: Action.Append) = {
        a.headers.toList.traverse { case (key, value) =>
          for {
            bytes <- FromTry[F].unsafe { value.toBytes }
          } yield {
            Header(key, bytes)
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


  implicit def consumerRecordToActionHeader[F[_] : FromTry/*TODO*/](implicit
    fromBytes: FromBytes[ActionHeader]
  ): Conversion[Option, ConsumerRecord[Id, ByteVector], F[ActionHeader]] = {
    a: ConsumerRecord[Id, ByteVector] => {
      for {
        header <- a.headers.find { _.key == `journal.action` }
      } yield {
        FromTry[F].unsafe { fromBytes(header.value) }
      }
    }
  }


  implicit def consumerRecordToActionRecord[F[_] : Monad : FromTry](implicit
    consumerRecordToActionHeader: Conversion[Option, ConsumerRecord[Id, ByteVector], F[ActionHeader]]
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
                for {
                  value <- FromTry[F].unsafe { header.value.fromBytes[String] } // TODO
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
