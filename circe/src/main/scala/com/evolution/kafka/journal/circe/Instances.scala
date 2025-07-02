package com.evolution.kafka.journal.circe

import cats.syntax.all.*
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.PayloadAndType.*
import com.evolution.kafka.journal.circe.Codecs.*
import com.evolution.kafka.journal.conversions.*
import com.evolution.kafka.journal.eventual.EventualRead
import com.evolutiongaming.catshelper.*
import io.circe.*
import io.circe.jawn.*
import io.circe.syntax.*
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

object Instances {

  implicit def kafkaWrite[F[_]: MonadThrowable](
    implicit
    payloadJsonToBytes: ToBytes[F, PayloadJson[Json]],
  ): KafkaWrite[F, Json] =
    KafkaWrite.writeJson(EventJsonPayloadAndType(_, PayloadType.Json), payloadJsonToBytes)

  implicit def kafkaRead[F[_]: MonadThrowable](
    implicit
    payloadJsonFromBytes: FromBytes[F, PayloadJson[Json]],
  ): KafkaRead[F, Json] =
    KafkaRead.readJson(payloadJsonFromBytes, (json: EventJsonPayloadAndType[Json]) => json.payload.pure[F])

  implicit def eventualRead[F[_]: MonadThrowable]: EventualRead[F, Json] =
    EventualRead.readJson(str => FromCirceResult.summon[F].apply(parse(str)))

  implicit def payloadJsonToBytes[F[_]: FromTry]: ToBytes[F, PayloadJson[Json]] = fromEncoder

  implicit def payloadJsonFromBytes[F[_]: ApplicativeThrowable]: FromBytes[F, PayloadJson[Json]] = fromDecoder

  private def fromEncoder[F[_]: FromTry, A: Encoder]: ToBytes[F, A] =
    a =>
      FromTry[F].unsafe {
        val json = a.asJson
        val byteBuffer = Printer.noSpaces.printToByteBuffer(json, StandardCharsets.UTF_8)
        ByteVector.view(byteBuffer)
      }

  private def fromDecoder[F[_]: ApplicativeThrowable, A: Decoder]: FromBytes[F, A] =
    bytes =>
      FromCirceResult
        .summon[F]
        .apply(decodeByteBuffer[A](bytes.toByteBuffer))
        .adaptErr { case e => JournalError(s"Failed to parse $bytes json: $e", e) }

}
