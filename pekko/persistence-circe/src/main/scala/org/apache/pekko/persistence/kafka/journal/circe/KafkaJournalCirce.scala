package org.apache.pekko.persistence.kafka.journal.circe

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.circe.FromCirceResult
import com.evolution.kafka.journal.util.Fail
import com.typesafe.config.Config
import io.circe.*
import io.circe.generic.semiauto.*
import io.circe.syntax.*
import org.apache.pekko.persistence.kafka.journal.*
import org.apache.pekko.persistence.kafka.journal.EventSerializer.PersistentRepresentation
import org.apache.pekko.persistence.kafka.journal.circe.KafkaJournalCirce.*

class KafkaJournalCirce(config: Config) extends KafkaJournal(config) {

  override def adapterIO: Resource[IO, JournalAdapter[IO]] = {
    for {
      serializer <- circeEventSerializer
      journalReadWrite <- circeJournalReadWrite.toResource
      adapter <- adapterIO(serializer, journalReadWrite)
    } yield adapter
  }

  def circeEventSerializer: Resource[IO, EventSerializer[IO, Json]] = {
    JsonEventSerializer
      .of[IO]
      .pure[Resource[IO, *]]
  }

  def circeJournalReadWrite: IO[JournalReadWrite[IO, Json]] =
    JournalReadWrite
      .of[IO, Json]
      .pure[IO]
}

object KafkaJournalCirce {

  implicit def persistentJsonEncoder[A: Encoder]: Encoder[PersistentJson[A]] = deriveEncoder
  implicit def persistentJsonDecoder[A: Decoder]: Decoder[PersistentJson[A]] = deriveDecoder

  object JsonEventSerializer {

    def of[F[_]: MonadThrowable: FromCirceResult]: EventSerializer[F, Json] = {

      def toEventPayload(repr: PersistentRepresentation): F[Json] = {

        def json(json: Json, payloadType: Option[PayloadType.TextOrJson] = None) = {
          val persistent = PersistentJson(
            manifest = repr.manifest,
            writerUuid = repr.writerUuid,
            payloadType = payloadType,
            payload = json,
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
        val fromCirceResult = FromCirceResult.summon[F]

        for {
          persistentJson <- fromCirceResult(json.as[PersistentJson[Json]])
          payloadType = persistentJson.payloadType getOrElse PayloadType.Json
          payload = persistentJson.payload
          anyRef <- payloadType match {
            case PayloadType.Text => fromCirceResult(payload.as[String]).widen[AnyRef]
            case PayloadType.Json => payload.pure[F].widen[AnyRef]
          }
        } yield {
          PersistentRepresentation(
            payload = anyRef,
            manifest = persistentJson.manifest,
            writerUuid = persistentJson.writerUuid,
          )
        }
      }

      EventSerializer(toEventPayload, fromEventPayload)
    }
  }

}
