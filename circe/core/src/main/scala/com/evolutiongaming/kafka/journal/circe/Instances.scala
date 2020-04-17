package com.evolutiongaming.kafka.journal.circe

import java.nio.charset.StandardCharsets

import cats.implicits._
import com.evolutiongaming.catshelper._
import com.evolutiongaming.kafka.journal.PayloadAndType._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.circe.Codecs._
import com.evolutiongaming.kafka.journal.conversions._
import com.evolutiongaming.kafka.journal.eventual.EventualRead
import io.circe._
import io.circe.jawn._
import io.circe.syntax._
import scodec.bits.ByteVector

object Instances {

  implicit def kafkaWrite[F[_] : MonadThrowable](
    implicit payloadJsonToBytes: ToBytes[F, PayloadJson[Json]]
  ): KafkaWrite[F, Json] =
    KafkaWrite.writeJson(EventJsonPayloadAndType(_, PayloadType.Json), payloadJsonToBytes)

  implicit def kafkaRead[F[_] : MonadThrowable](
    implicit payloadJsonFromBytes: FromBytes[F, PayloadJson[Json]]
  ): KafkaRead[F, Json] =
    KafkaRead.readJson(payloadJsonFromBytes, (json: EventJsonPayloadAndType[Json]) => json.payload.pure[F])

  implicit def eventualRead[F[_] : MonadThrowable]: EventualRead[F, Json] =
    EventualRead.readJson(str => FromCirceResult[F].apply(parse(str)))

  implicit def payloadJsonToBytes[F[_]: FromTry]: ToBytes[F, PayloadJson[Json]] = fromEncoder

  implicit def payloadJsonFromBytes[F[_]: ApplicativeThrowable]: FromBytes[F, PayloadJson[Json]] = fromDecoder

  private def fromEncoder[F[_] : FromTry, A : Encoder]: ToBytes[F, A] =
    a => FromTry[F].unsafe {
      val json = a.asJson
      val byteBuffer = Printer.noSpaces.printToByteBuffer(json, StandardCharsets.UTF_8)
      ByteVector.view(byteBuffer)
    }

  private def fromDecoder[F[_] : ApplicativeThrowable, A : Decoder]: FromBytes[F, A] =
    bytes =>
      FromCirceResult[F].apply(decodeByteBuffer[A](bytes.toByteBuffer))
        .adaptErr { case e => JournalError(s"Failed to parse $bytes json: $e", e) }

}
