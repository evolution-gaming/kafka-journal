package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import cats.effect.Sync
import cats.implicits._
import cats.{Applicative, Monad, ~>}
import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.kafka.journal.FromBytes.implicits._
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits._
import com.evolutiongaming.kafka.journal.ToBytes.implicits._
import com.evolutiongaming.kafka.journal._
import play.api.libs.json.{JsString, JsValue, Json}
import scodec.bits.ByteVector

trait EventSerializer[F[_], A] {

  def toEvent(persistentRepr: PersistentRepr): F[Event[A]]
  
  def toPersistentRepr(persistenceId: PersistenceId, event: Event[A]): F[PersistentRepr]
}

object EventSerializer {

  def const[F[_] : Applicative, A](event: Event[A], persistentRepr: PersistentRepr): EventSerializer[F, A] = {
    new EventSerializer[F, A] {

      def toEvent(persistentRepr: PersistentRepr) = event.pure[F]

      def toPersistentRepr(persistenceId: PersistenceId, event: Event[A]) = persistentRepr.pure[F]
    }
  }


  def of[F[_] : Sync : FromTry : FromAttempt : FromJsResult](system: ActorSystem): F[EventSerializer[F, Payload]] = {
    implicit val fail = Fail.lift[F]
    for {
      serializedMsgSerializer <- SerializedMsgSerializer.of[F](system)
    } yield {
      apply[F](serializedMsgSerializer)
    }
  }


  def apply[F[_] : Monad : FromAttempt : FromJsResult : Fail](
    serializer: SerializedMsgSerializer[F]
  ): EventSerializer[F, Payload] = {

    new EventSerializer[F, Payload] {

      def toEvent(persistentRepr: PersistentRepr) = {
        val tagged = PersistentReprPayload(persistentRepr)

        def binary(payload: AnyRef) = {
          for {
            serialized <- serializer.toMsg(payload)
            persistent  = PersistentBinary(serialized, persistentRepr)
            bytes      <- persistent.toBytes[F]
          } yield {
            Payload.binary(bytes)
          }
        }

        def json(payload: JsValue, payloadType: Option[PayloadType.TextOrJson] = None) = {
          val manifest = ManifestOf(persistentRepr)
          val persistent = PersistentJson(
            manifest = manifest,
            writerUuid = persistentRepr.writerUuid,
            payloadType = payloadType,
            payload = payload)
          val json = Json.toJson(persistent)
          Payload.json(json)
        }

        val payload = tagged.payload match {
          case payload: JsValue => json(payload).pure[F]
          case payload: String  => json(JsString(payload), PayloadType.Text.some).pure[F]
          case payload: AnyRef  => binary(payload)
        }
        for {
          payload <- payload
          seqNr   <- SeqNr.of[F](persistentRepr.sequenceNr)
        } yield {
          Event(seqNr, tagged.tags, payload.some)
        }
      }

      def toPersistentRepr(persistenceId: PersistenceId, event: Event[Payload]) = {

        def persistentRepr(payload: AnyRef, writerUuid: String, manifest: Option[String]) = {
          PersistentRepr(
            payload = payload,
            sequenceNr = event.seqNr.value,
            persistenceId = persistenceId,
            manifest = manifest getOrElse PersistentRepr.Undefined,
            writerUuid = writerUuid)
        }

        def binary(payload: ByteVector) = {
          for {
            persistent <- payload.fromBytes[F, PersistentBinary]
            anyRef     <- serializer.fromMsg(persistent.payload)
          } yield {
            persistentRepr(
              payload = anyRef,
              manifest = persistent.manifest,
              writerUuid = persistent.writerUuid)
          }
        }

        def json(payload: JsValue) = {
          for {
            persistent  <- FromJsResult[F].apply { payload.validate[PersistentJson[JsValue]] }
            payloadType  = persistent.payloadType getOrElse PayloadType.Json
            anyRef      <- payloadType match {
              case PayloadType.Text => FromJsResult[F].apply { persistent.payload.validate[String].map(a => a: AnyRef) }
              case PayloadType.Json => persistent.payload.pure[F]
            }
          } yield {
            persistentRepr(
              payload = anyRef,
              manifest = persistent.manifest,
              writerUuid = persistent.writerUuid)
          }
        }

        val payload = event.payload.fold {
          s"Event.payload is not defined, persistenceId: $persistenceId, event: $event".fail[F, Payload]
        } {
          _.pure[F]
        }

        for {
          payload        <- payload
          persistentRepr <- payload match {
            case p: Payload.Binary => binary(p.value)
            case _: Payload.Text   => s"Payload.Text is not supported, persistenceId: $persistenceId, event: $event".fail[F, PersistentRepr]
            case p: Payload.Json   => json(p.value)
          }
        } yield persistentRepr
      }
    }
  }

  implicit class EventSerializerOps[F[_], A](val self: EventSerializer[F, A]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): EventSerializer[G, A] = new EventSerializer[G, A] {

      def toEvent(persistentRepr: PersistentRepr) = {
        f(self.toEvent(persistentRepr))
      }

      def toPersistentRepr(persistenceId: PersistenceId, event: Event[A]) = {
        f(self.toPersistentRepr(persistenceId, event))
      }
    }
  }
}
