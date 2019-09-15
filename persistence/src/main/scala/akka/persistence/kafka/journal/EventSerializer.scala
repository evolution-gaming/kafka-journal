package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import cats.effect.Sync
import cats.implicits._
import cats.{Applicative, MonadError, ~>}
import com.evolutiongaming.catshelper.{FromTry, MonadThrowable}
import com.evolutiongaming.kafka.journal.FromBytes.implicits._
import com.evolutiongaming.kafka.journal.ToBytes.implicits._
import com.evolutiongaming.kafka.journal._
import play.api.libs.json.{JsString, JsValue, Json}
import scodec.bits.ByteVector

trait EventSerializer[F[_]] {

  def toEvent(persistentRepr: PersistentRepr): F[Event]
  
  def toPersistentRepr(persistenceId: PersistenceId, event: Event): F[PersistentRepr]
}

object EventSerializer {

  def const[F[_] : Applicative](event: Event, persistentRepr: PersistentRepr): EventSerializer[F] = {
    new EventSerializer[F] {

      def toEvent(persistentRepr: PersistentRepr) = event.pure[F]

      def toPersistentRepr(persistenceId: PersistenceId, event: Event) = persistentRepr.pure[F]
    }
  }


  def of[F[_] : Sync : FromTry : FromAttempt : FromJsResult](system: ActorSystem): F[EventSerializer[F]] = {
    for {
      serializedMsgSerializer <- SerializedMsgSerializer.of[F](system)
    } yield {
      apply[F](serializedMsgSerializer)
    }
  }
  

  def apply[F[_] : MonadThrowable : FromAttempt : FromJsResult](
    serializer: SerializedMsgSerializer[F]
  ): EventSerializer[F] = new EventSerializer[F] {

    def toEvent(persistentRepr: PersistentRepr) = {
      val (anyRef: AnyRef, tags) = PayloadAndTags(persistentRepr.payload)

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

      val payload = anyRef match {
        case payload: JsValue => json(payload).pure[F]
        case payload: String  => json(JsString(payload), Some(PayloadType.Text)).pure[F]
        case payload          => binary(payload)
      }
      for {
        payload <- payload
        seqNr   <- MonadError[F, Throwable].catchNonFatal { SeqNr.unsafe(persistentRepr.sequenceNr) }
      } yield {
        Event(seqNr, tags, Some(payload))
      }
    }

    def toPersistentRepr(persistenceId: PersistenceId, event: Event) = {

      def error[A](msg: String) = {
        val error = JournalError(msg)
        error.raiseError[F, A]
      }

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
        error[Payload](s"Event.payload is not defined, persistenceId: $persistenceId, event: $event")
      } {
        _.pure[F]
      }

      for {
        payload        <- payload
        persistentRepr <- payload match {
          case p: Payload.Binary => binary(p.value)
          case _: Payload.Text   => error[PersistentRepr](s"Payload.Text is not supported, persistenceId: $persistenceId, event: $event")
          case p: Payload.Json   => json(p.value)
        }
      } yield persistentRepr
    }
  }


  implicit class EventSerializerOps[F[_]](val self: EventSerializer[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): EventSerializer[G] = new EventSerializer[G] {

      def toEvent(persistentRepr: PersistentRepr) = {
        f(self.toEvent(persistentRepr))
      }

      def toPersistentRepr(persistenceId: PersistenceId, event: Event) = {
        f(self.toPersistentRepr(persistenceId, event))
      }
    }
  }
}
