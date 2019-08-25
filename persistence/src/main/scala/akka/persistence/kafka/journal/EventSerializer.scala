package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import cats.effect.Sync
import cats.implicits._
import cats.{Applicative, ~>}
import com.evolutiongaming.catshelper.FromTry
import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
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


  def of[F[_] : Sync : FromTry/*TODO*/](system: ActorSystem): F[EventSerializer[F]] = {
    for {
      serializedMsgSerializer <- SerializedMsgSerializer.of[F](system)
    } yield {
      apply[F](serializedMsgSerializer)
    }
  }
  

  def apply[F[_] : Sync : FromTry/*TODO*/](
    serializer: SerializedMsgSerializer[F]/*TODO*/
  ): EventSerializer[F] = new EventSerializer[F] {

    def toEvent(persistentRepr: PersistentRepr) = {
      val (anyRef: AnyRef, tags) = PayloadAndTags(persistentRepr.payload)

      def binary(payload: AnyRef) = {
        for {
          serialized <- serializer.toMsg(payload)
          persistent  = PersistentBinary(serialized, persistentRepr)
          bytes      <- FromTry[F].unsafe { persistent.toBytes }
        } yield {
          Payload.binary(ByteVector.view(bytes))
        }
      }

      def json(payload: JsValue, payloadType: Option[PayloadType.TextOrJson] = None) = {
        val manifest = ManifestOf(persistentRepr)
        val persistent = PersistentJson(
          manifest = manifest,
          writerUuid = persistentRepr.writerUuid,
          payloadType = payloadType,
          payload = payload)
        for {
          json <- FromTry[F].unsafe { Json.toJson(persistent) }
        } yield {
          Payload.json(json)
        }
      }

      val payload = anyRef match {
        case payload: JsValue => json(payload)
        case payload: String  => json(JsString(payload), Some(PayloadType.Text))
        case payload          => binary(payload)
      }
      for {
        payload <- payload
        seqNr   <- FromTry[F].unsafe { SeqNr(persistentRepr.sequenceNr) }
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
          persistent <- FromTry[F].unsafe { payload.toArray.fromBytes[PersistentBinary] }
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
          persistent  <- FromTry[F].unsafe { payload.as[PersistentJson] } // TODO not use `as`
          payloadType  = persistent.payloadType getOrElse PayloadType.Json
          anyRef      <- payloadType match {
            case PayloadType.Text => FromTry[F].unsafe { persistent.payload.as[String] : AnyRef }  // TODO not use `as`
            case PayloadType.Json => (persistent.payload : AnyRef).pure[F]
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
