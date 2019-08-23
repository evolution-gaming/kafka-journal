package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import cats.MonadError
import cats.implicits._
import com.evolutiongaming.catshelper.FromTry
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
    apply[cats.Id](system)
  }

  def apply[F[_]](system: ActorSystem)(implicit F: MonadError[F, Throwable]): EventSerializer[F] = {
    apply[F](SerializedMsgExt(system))
  }

  def apply[F[_] : FromTry](
    serialisation: SerializedMsgConverter/*TODO*/)(implicit
    F: MonadError[F, Throwable]
  ): EventSerializer[F] = new EventSerializer[F] {

    def toEvent(persistentRepr: PersistentRepr) = {
      val (anyRef: AnyRef, tags) = PayloadAndTags(persistentRepr.payload)

      def binary(payload: AnyRef) = {
        val serialized = serialisation.toMsg(payload)
        val persistent = PersistentBinary(serialized, persistentRepr)
        val bytes = persistent.toBytes
        Payload.binary(ByteVector.view(bytes))
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
        case payload: JsValue => json(payload)
        case payload: String  => json(JsString(payload), Some(PayloadType.Text))
        case payload          => binary(payload)
      }
      val seqNr = SeqNr(persistentRepr.sequenceNr)
      val event = Event(seqNr, tags, Some(payload))
      event.pure[F]
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
        val persistent = payload.toArray.fromBytes[PersistentBinary]
        val anyRef = serialisation.fromMsg(persistent.payload).get
        PersistentRepr(
          payload = anyRef,
          sequenceNr = event.seqNr.value,
          persistenceId = persistenceId,
          manifest = persistent.manifest getOrElse PersistentRepr.Undefined,
          writerUuid = persistent.writerUuid)
      }

      def json(payload: JsValue) = {
        val persistent = payload.as[PersistentJson]
        val payloadType = persistent.payloadType getOrElse PayloadType.Json
        val anyRef: AnyRef = payloadType match {
          case PayloadType.Text => persistent.payload.as[String]
          case PayloadType.Json => persistent.payload
        }
        PersistentRepr(
          payload = anyRef,
          sequenceNr = event.seqNr.value,
          persistenceId = persistenceId,
          manifest = persistent.manifest getOrElse PersistentRepr.Undefined,
          writerUuid = persistent.writerUuid)
      }

      for {
        payload        <- payload
        persistentRepr <- payload match {
          case p: Payload.Binary => binary(p.value).pure[F]
          case _: Payload.Text   => error[PersistentRepr](s"Payload.Text is not supported, persistenceId: $persistenceId, event: $event")
          case p: Payload.Json   => json(p.value).pure[F]
        }
      } yield persistentRepr
    }
  }
}
