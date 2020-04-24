package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import cats.effect.Sync
import cats.implicits._
import cats.{Applicative, ~>}
import com.evolutiongaming.catshelper.{FromTry, MonadThrowable}
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

  final case class PersistentRepresentation(payload: Any, writerUuid: String, manifest: Option[String])

  def const[F[_] : Applicative, A](event: Event[A], persistentRepr: PersistentRepr): EventSerializer[F, A] = {
    new EventSerializer[F, A] {

      def toEvent(persistentRepr: PersistentRepr) = event.pure[F]

      def toPersistentRepr(persistenceId: PersistenceId, event: Event[A]) = persistentRepr.pure[F]
    }
  }


  def of[F[_] : Sync : FromTry : FromAttempt : FromJsResult](system: ActorSystem): F[EventSerializer[F, Payload]] = {
    for {
      serializedMsgSerializer <- SerializedMsgSerializer.of[F](system)
    } yield {
      apply[F](serializedMsgSerializer)
    }
  }


  def apply[F[_] : MonadThrowable : FromAttempt : FromJsResult](
    serializer: SerializedMsgSerializer[F]
  ): EventSerializer[F, Payload] = {

    def toEventPayload(repr: PersistentRepresentation): F[Payload] = {

      def binary(payload: AnyRef) = {
        for {
          serialized <- serializer.toMsg(payload)
          persistent  = PersistentBinary(repr.manifest, repr.writerUuid, serialized)
          bytes      <- persistent.toBytes[F]
        } yield {
          Payload.binary(bytes)
        }
      }

      def json(payload: JsValue, payloadType: Option[PayloadType.TextOrJson] = None) = {
        val persistent = PersistentJson(
          manifest = repr.manifest,
          writerUuid = repr.writerUuid,
          payloadType = payloadType,
          payload = payload)
        val json = Json.toJson(persistent)
        Payload.json(json)
      }

      repr.payload match {
        case payload: JsValue => json(payload).pure[F]
        case payload: String  => json(JsString(payload), PayloadType.Text.some).pure[F]
        case payload: AnyRef  => binary(payload)
      }
    }

    def fromEventPayload(payload: Payload): F[PersistentRepresentation] = {

      def binary(payload: ByteVector): F[PersistentRepresentation] = {
        for {
          persistent <- payload.fromBytes[F, PersistentBinary]
          anyRef     <- serializer.fromMsg(persistent.payload)
        } yield {
          PersistentRepresentation(
            payload = anyRef,
            manifest = persistent.manifest,
            writerUuid = persistent.writerUuid
          )
        }
      }

      def json(payload: JsValue): F[PersistentRepresentation] = {
        for {
          persistent  <- FromJsResult[F].apply { payload.validate[PersistentJson[JsValue]] }
          payloadType  = persistent.payloadType getOrElse PayloadType.Json
          anyRef      <- payloadType match {
            case PayloadType.Text => FromJsResult[F].apply { persistent.payload.validate[String].map(a => a: AnyRef) }
            case PayloadType.Json => persistent.payload.pure[F].widen[AnyRef]
          }
        } yield {
          PersistentRepresentation(
            payload = anyRef,
            manifest = persistent.manifest,
            writerUuid = persistent.writerUuid
          )
        }
      }

      payload match {
        case p: Payload.Binary => binary(p.value)
        case _: Payload.Text   => Fail.lift[F].fail(s"Payload.Text is not supported")
        case p: Payload.Json   => json(p.value)
      }
    }

    EventSerializer(toEventPayload, fromEventPayload)
  }

  def apply[F[_] : MonadThrowable, A](
    toEventPayload: PersistentRepresentation => F[A],
    fromEventPayload: A => F[PersistentRepresentation]
  ): EventSerializer[F, A] = new EventSerializer[F, A] {

    implicit val fail: Fail[F] = Fail.lift[F]

    override def toEvent(persistentRepr: PersistentRepr): F[Event[A]] = {

      val tagged = PersistentReprPayload(persistentRepr)
      val manifest = ManifestOf(persistentRepr)

      val persistentRepresentation = PersistentRepresentation(
        tagged.payload,
        persistentRepr.writerUuid,
        manifest
      )

      val result = for {
        payload <- toEventPayload(persistentRepresentation)
        seqNr   <- SeqNr.of[F](persistentRepr.sequenceNr)
      } yield {
        Event(seqNr, tagged.tags, payload.some)
      }

      result.adaptErr { case e =>
        JournalError(s"ToEvent error, persistenceId: ${persistentRepr.persistenceId}: $e", e)
      }
    }

    override def toPersistentRepr(persistenceId: PersistenceId, event: Event[A]): F[PersistentRepr] = {

      def persistentRepr(repr: PersistentRepresentation) = {
        PersistentRepr(
          payload = repr.payload,
          sequenceNr = event.seqNr.value,
          persistenceId = persistenceId,
          manifest = repr.manifest getOrElse PersistentRepr.Undefined,
          writerUuid = repr.writerUuid)
      }

      val payload = event.payload.fold {
        s"Event.payload is not defined".fail[F, A]
      } {
        _.pure[F]
      }

      val result = for {
        payload <- payload
        persistentPayload <- fromEventPayload(payload)
      } yield persistentRepr(persistentPayload)

      result.adaptErr { case e =>
        JournalError(s"ToPersistentRepr error, persistenceId: $persistenceId, event: $event: $e", e)
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
