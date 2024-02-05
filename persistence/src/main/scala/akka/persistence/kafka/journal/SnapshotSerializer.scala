package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.{SelectedSnapshot, SnapshotMetadata}
import cats.effect.kernel.Sync
import cats.syntax.all._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.FromBytes.implicits._
import com.evolutiongaming.kafka.journal.ToBytes.implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.serialization.SerializedMsg
import play.api.libs.json.{JsString, JsValue, Json}
import scodec.bits.ByteVector

import java.time.Instant

trait SnapshotSerializer[F[_], A] {

  def toInternalRepresentation(metadata: SnapshotMetadata, snapshot: Any): F[Snapshot[A]]
  def toAkkaRepresentation(persistenceId: PersistenceId, timestamp: Instant, snapshot: Snapshot[A]): F[SelectedSnapshot]

}

object SnapshotSerializer {

  def of[F[_]: Sync: FromAttempt: FromJsResult](system: ActorSystem): F[SnapshotSerializer[F, Payload]] =
    SerializedMsgSerializer.of[F](system).map(SnapshotSerializer(_))

  def apply[F[_]: MonadThrowable: FromAttempt: FromJsResult](
      serializer: SerializedMsgSerializer[F]
  ): SnapshotSerializer[F, Payload] = {

    implicit val toBytesSerializedMsg: ToBytes[F, SerializedMsg] = ToBytes.fromEncoder
    implicit val fromBytesSerializedMsg: FromBytes[F, SerializedMsg] = FromBytes.fromDecoder

    def toSnapshotPayload(payload: Any): F[Payload] = {

      def binary(payload: AnyRef) =
        // TODO: should we use PersitentBinary?
        for {
          serializedMsg <- serializer.toMsg(payload)
          bytes <- serializedMsg.toBytes[F]
        } yield Payload.binary(bytes)

      def json(payload: JsValue, payloadType: Option[PayloadType.TextOrJson] = None) = {
        // TODO: should we use another structure?
        val persistent = PersistentJson(manifest = None, writerUuid = "", payloadType = payloadType, payload = payload)
        val json = Json.toJson(persistent)
        Payload.json(json)
      }

      // TODO: what will happen if `payload` is `Any`?
      payload match {
        case payload: JsValue => json(payload).pure[F]
        case payload: String  => json(JsString(payload), PayloadType.Text.some).pure[F]
        case payload: AnyRef  => binary(payload)
      }
    }

    def fromSnapshotPayload(payload: Payload): F[Any] = {

      def binary(payload: ByteVector): F[Any] = {
        for {
          serializedMsg <- payload.fromBytes[F, SerializedMsg]
          anyRef <- serializer.fromMsg(serializedMsg)
        } yield anyRef
      }

      def json(payload: JsValue): F[Any] = {
        for {
          persistent <- FromJsResult[F].apply(payload.validate[PersistentJson[JsValue]])
          payloadType = persistent.payloadType.getOrElse(PayloadType.Json)
          anyRef <- payloadType match {
            case PayloadType.Text => FromJsResult[F].apply(persistent.payload.validate[String])
            case PayloadType.Json => persistent.payload.pure[F].widen[AnyRef]
          }
        } yield anyRef
      }

      payload match {
        case p: Payload.Binary => binary(p.value)
        case _: Payload.Text   => Fail.lift[F].fail(s"Payload.Text is not supported")
        case p: Payload.Json   => json(p.value)
      }
    }

    SnapshotSerializer(toSnapshotPayload, fromSnapshotPayload)
  }

  def apply[F[_]: MonadThrowable, A](
      toSnapshotPayload: Any => F[A],
      fromSnapshotPayload: A => F[Any]
  ): SnapshotSerializer[F, A] = new SnapshotSerializer[F, A] {

    implicit val fail: Fail[F] = Fail.lift[F]

    def toInternalRepresentation(metadata: SnapshotMetadata, snapshot: Any): F[Snapshot[A]] = {

      val result = for {
        payload <- toSnapshotPayload(snapshot)
        seqNr <- SeqNr.of[F](metadata.sequenceNr)
      } yield Snapshot(seqNr, payload)

      result.adaptErr { case e =>
        SnapshotStoreError(s"ToSnapshot error, persistenceId: ${metadata.persistenceId}: $e", e)
      }
    }

    def toAkkaRepresentation(
        persistenceId: PersistenceId,
        timestamp: Instant,
        snapshot: Snapshot[A]
    ): F[SelectedSnapshot] = {

      val payload = fromSnapshotPayload(snapshot.payload)

      val result = payload.map { payload =>
        SelectedSnapshot(
          metadata = SnapshotMetadata(
            persistenceId = persistenceId,
            sequenceNr = snapshot.seqNr.value,
            timestamp = timestamp.toEpochMilli
          ),
          snapshot = payload
        )
      }

      result.adaptErr { case e =>
        SnapshotStoreError(s"FromSnapshot error, persistenceId: $persistenceId, snapshot: $snapshot: $e", e)
      }
    }
  }

}
