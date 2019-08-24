package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import cats.effect.{IO, Sync}
import cats.implicits._
import cats.{FlatMap, ~>}
import com.evolutiongaming.catshelper.{FromTry, ToTry}
import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.serialization.{SerializedMsgConverter, SerializedMsgExt}
import play.api.libs.json.{JsString, JsValue, Json}
import scodec.bits.ByteVector

trait EventSerializer[F[_]] {

  def toEvent(persistentRepr: PersistentRepr): F[Event]
  
  def toPersistentRepr(persistenceId: PersistenceId, event: Event): F[PersistentRepr]
}

object EventSerializer {

  // TODO remove
  def unsafe(system: ActorSystem): EventSerializer[cats.Id] = {
    implicit val monadError = MonadErrorOf.throwable[cats.Id]
    val eventSerializer = apply[IO](system)
    val unsafe = new (IO ~> cats.Id) {
      def apply[A](fa: IO[A]) = ToTry[IO].apply(fa).get
    }
    eventSerializer.mapK(unsafe)
  }

  def apply[F[_] : Sync : FromTry](system: ActorSystem): EventSerializer[F] = {
    apply[F](SerializedMsgExt(system)) // TODO wrap
  }

  def apply[F[_] : Sync : FromTry](
    serialisation: SerializedMsgConverter/*TODO*/
  ): EventSerializer[F] = new EventSerializer[F] {

    def toEvent(persistentRepr: PersistentRepr) = {
      val (anyRef: AnyRef, tags) = PayloadAndTags(persistentRepr.payload)

      def binary(payload: AnyRef) = {

        for {
          serialized <- Sync[F].delay { serialisation.toMsg(payload) }
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

      val payload = event.payload.fold {
        error[Payload](s"Event.payload is not defined, persistenceId: $persistenceId, event: $event")
      } {
        _.pure[F]
      }

      def binary(payload: ByteVector) = {
        for {
          persistent <- FromTry[F].unsafe { payload.toArray.fromBytes[PersistentBinary] }
          anyRef     <- FromTry[F].apply { serialisation.fromMsg(persistent.payload) }
        } yield {        PersistentRepr(
          payload = anyRef,
          sequenceNr = event.seqNr.value,
          persistenceId = persistenceId,
          manifest = persistent.manifest getOrElse PersistentRepr.Undefined,
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
          PersistentRepr(
            payload = anyRef,
            sequenceNr = event.seqNr.value,
            persistenceId = persistenceId,
            manifest = persistent.manifest getOrElse PersistentRepr.Undefined,
            writerUuid = persistent.writerUuid)
        }
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

    def mapK[G[_] : FlatMap](f: F ~> G): EventSerializer[G] = new EventSerializer[G] {

      def toEvent(persistentRepr: PersistentRepr) = {
        f(self.toEvent(persistentRepr))
      }

      def toPersistentRepr(persistenceId: PersistenceId, event: Event) = {
        f(self.toPersistentRepr(persistenceId, event))
      }
    }
  }
}
