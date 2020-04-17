package com.evolutiongaming.kafka.journal.circe

import akka.persistence.kafka.journal.EventSerializer.PersistentRepresentation
import akka.persistence.kafka.journal._
import cats.effect.{IO, Resource}
import cats.implicits._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.circe.Codecs._
import com.evolutiongaming.kafka.journal.circe.Instances._
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.circe.KafkaJournalCirce.JsonEventSerializer
import com.typesafe.config.Config
import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._

class KafkaJournalCirce(config: Config) extends KafkaJournal(config) {

  override def adapterIO: Resource[IO, JournalAdapter[IO]] = {
    for {
      serializer       <- circeEventSerializer
      journalReadWrite <- Resource.liftF(circeJournalReadWrite)
      adapter          <- adapterIO(serializer, journalReadWrite)
    } yield adapter
  }

  def circeEventSerializer: Resource[IO, EventSerializer[IO, Json]] = {
    val serializer = JsonEventSerializer.of[IO].pure[IO]
    Resource.liftF(serializer)
  }

  def circeJournalReadWrite: IO[JournalReadWrite[IO, Json]] =
    JournalReadWrite.of[IO, Json].pure[IO]
}

object KafkaJournalCirce {

  implicit def persistentJsonEncoder[A : Encoder]: Encoder[PersistentJson[A]] = deriveEncoder
  implicit def persistentJsonDecoder[A : Decoder]: Decoder[PersistentJson[A]] = deriveDecoder

  object JsonEventSerializer {

    def of[F[_] : MonadThrowable : FromCirceResult]: EventSerializer[F, Json] = {

      def toEventPayload(repr: PersistentRepresentation): F[Json] = {

        def json(json: Json, payloadType: Option[PayloadType.TextOrJson] = None) = {
          val persistent = PersistentJson(
            manifest = repr.manifest,
            writerUuid = repr.writerUuid,
            payloadType = payloadType,
            payload = json
          )
          persistent.asJson.dropNullValues
        }

        repr.payload match {
          case payload: Json => json(payload).pure[F]
          case payload: String => json(Json.fromString(payload), PayloadType.Text.some).pure[F]
          case other => Fail.lift[F].fail(s"Event.payload is not supported, payload: $other")
        }
      }

      def fromEventPayload(json: Json): F[PersistentRepresentation] = {
        for {
          persistentJson <- FromCirceResult[F].apply(json.as[PersistentJson[Json]])
          payloadType = persistentJson.payloadType getOrElse PayloadType.Json
          payload = persistentJson.payload
          anyRef <- payloadType match {
            case PayloadType.Text => FromCirceResult[F].apply(payload.as[String]).widen[AnyRef]
            case PayloadType.Json => payload.pure[F].widen[AnyRef]
          }
        } yield {
          PersistentRepresentation(
            payload = anyRef,
            manifest = persistentJson.manifest,
            writerUuid = persistentJson.writerUuid
          )
        }
      }

      EventSerializer(toEventPayload, fromEventPayload)
    }
  }

}
