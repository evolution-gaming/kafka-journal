package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import akka.persistence.PersistentRepr
import cats.effect.Sync
import cats.syntax.all.*
import cats.{Applicative, ~>}
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.FromBytes.implicits.*
import com.evolutiongaming.kafka.journal.ToBytes.implicits.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits.*
import play.api.libs.json.{JsString, JsValue, Json}
import scodec.bits.ByteVector

/** Transforms persistent message between Akka representation and internal Kafka Journal format.
  * 
  * @tparam A
  *   Type of serialized payload. At the time of writing it could be either
  *   [[Payload]] by default, or [[Json]] if `kafka-journal-circe` module is
  *   used.
  */
trait EventSerializer[F[_], A] {

  /** Transform persistent Akka message to Kafka Journal specific internal representation.
    *
    * [[PersistentRepr#payload]] will get serialized to a form writable to Kafka and stored into [[Event#payload]].
    *
    * The method may raise an error into `F[_]` if it is not possible to serialize a message, i.e. for example if
    * [[PersistentRepr#sequenceNr]] is negative or equals to zero.
    */
  def toEvent(persistentRepr: PersistentRepr): F[Event[A]]

  /** Transform Kafka Journal event to persistent Akka message.
    *
    * Parse [[Event#payload]] read from either Kafka or eventual store (i.e. Cassandra).
    *
    * The method may raise an error into `F[_]` if parsing fails, the event payload is not found or the format is not
    * recognized.
    */
  def toPersistentRepr(persistenceId: PersistenceId, event: Event[A]): F[PersistentRepr]
}

object EventSerializer {

  /** Single Akka message before serialization to Kafka Journal event format.
    *
    * It represents three fields from [[PersistentRepr]] class, i.e.
    * [[PersistentRepr#payload]], [[PersistentRepr#writerUuid]] and
    * [[PersistentRepr#manifest]].
    *
    * The reason this class was created is to make the logic constructing
    * `Event[A]` in [[EventSerializer]] generic by having ability to convert from
    * an instance of [[PersistentRepresentation]] to `A` and back.
    *
    * To give an example, at the time of writing the following conversions were
    * available:
    * ```
    * PersistentRepresentation <-> Payload (in kafka-journal module)
    * PersistentRepresentation <-> Json    (in kafka-journal-circe module)
    * ```
    *
    * It might be possible to express the same logic without using the class, so
    * in future it might be removed as an overall simplification.
    *
    * It is usually serialized to a single `Payload` instance if Play JSON is
    * used, or `Json` instance if Circe is used instead.
    *
    * @param payload
    *   Non-serialized payload of an Akka message/event. See also
    *   [[PersistentRepr#payload]].
    * @param writerUuid
    *   Same as [[PersistentRepr#writerUuid]]
    * @param manifest
    *   Same as [[PersistentRepr#manifest]], but the lack of manifest is
    *   represented by `None` rather than `""`.
    */
  final case class PersistentRepresentation(payload: Any, writerUuid: String, manifest: Option[String])

  /** Always return a fixed `Event` or `PeristentRepr`.
    *
    * This could be useful in unit tests, when serialization mechanism is not
    * required.
    */
  def const[F[_]: Applicative, A](event: Event[A], persistentRepr: PersistentRepr): EventSerializer[F, A] = {
    new EventSerializer[F, A] {

      def toEvent(persistentRepr: PersistentRepr) = event.pure[F]

      def toPersistentRepr(persistenceId: PersistenceId, event: Event[A]) = persistentRepr.pure[F]
    }
  }

  /** Serialize [[PersistentRepr]] to [[Payload]] using Akka serialization or
    * Play JSON.
    *
    * @see
    *   [[[#apply[F[_]](serializer*]]] for more details on how serialization
    *   mechanism is determined.
    */
  def of[F[_]: Sync: FromAttempt: FromJsResult](system: ActorSystem): F[EventSerializer[F, Payload]] = {
    for {
      serializedMsgSerializer <- SerializedMsgSerializer.of[F](system)
    } yield {
      apply[F](serializedMsgSerializer)
    }
  }

  /** Serialize [[PersistentRepr]] to [[Payload]] using Akka serialization or
    * Play JSON.
    *
    * The underlying type of a resulting [[Payload]] will depend on
    * [[PersistentRepr#payload]] value. I.e. it will use Play JSON and create
    * [[Payload.Json]] if it is equal to [[JsValue]] and use Akka serialization
    * and create [[Payload.Binary]] for everything else.
    *
    * `SerializedMsgSerializer` is only used if [[Payload.Binary]] is to be
    * created or parsed. In this case also `FromAttempt` parameter is used to
    * append [[PersistentRepr#manifest]] and [[PersistentRepr#writerUuid]] into
    * a resulting [[ByteVector]].
    *
    * `FromJsResult` is used if [[Payload.Json]] is to be parsed. There is no
    * need for a reverse operation, because [[PersistentRepr#payload]] is
    * already a [[JsValue]] in this case.
    *
    * @note
    *   It might be useful to have a unit test checking that [[Payload.Binary]]
    *   is not created accidentially, because it might be too easy to make a
    *   mistake while setting a value to [[PersistentRepr#payload]] (which has
    *   `Any` type and will accept anything assigned).
    */
  def apply[F[_]: MonadThrowable: FromAttempt: FromJsResult](
    serializer: SerializedMsgSerializer[F],
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
        val persistent =
          PersistentJson(manifest = repr.manifest, writerUuid = repr.writerUuid, payloadType = payloadType, payload = payload)
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
            payload    = anyRef,
            manifest   = persistent.manifest,
            writerUuid = persistent.writerUuid,
          )
        }
      }

      def json(payload: JsValue): F[PersistentRepresentation] = {
        for {
          persistent <- FromJsResult[F].apply { payload.validate[PersistentJson[JsValue]] }
          payloadType = persistent.payloadType getOrElse PayloadType.Json
          anyRef <- payloadType match {
            case PayloadType.Text => FromJsResult[F].apply { persistent.payload.validate[String].map(a => a: AnyRef) }
            case PayloadType.Json => persistent.payload.pure[F].widen[AnyRef]
          }
        } yield {
          PersistentRepresentation(
            payload    = anyRef,
            manifest   = persistent.manifest,
            writerUuid = persistent.writerUuid,
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

  /** Serialize [[PersistentRepr]] to [[Payload]] using custom encoder/decoder.
    *
    * See `kafka-journal-circe` module for an example of how this method could
    * be used.
    */
  def apply[F[_]: MonadThrowable, A](
    toEventPayload: PersistentRepresentation => F[A],
    fromEventPayload: A => F[PersistentRepresentation],
  ): EventSerializer[F, A] = new EventSerializer[F, A] {

    implicit val fail: Fail[F] = Fail.lift[F]

    override def toEvent(persistentRepr: PersistentRepr): F[Event[A]] = {

      val tagged   = PersistentReprPayload(persistentRepr)
      val manifest = ManifestOf(persistentRepr)

      val persistentRepresentation = PersistentRepresentation(
        tagged.payload,
        persistentRepr.writerUuid,
        manifest,
      )

      val result = for {
        payload <- toEventPayload(persistentRepresentation)
        seqNr   <- SeqNr.of[F](persistentRepr.sequenceNr)
      } yield {
        Event(seqNr, tagged.tags, payload.some)
      }

      result.adaptErr {
        case e =>
          JournalError(s"ToEvent error, persistenceId: ${persistentRepr.persistenceId}: $e", e)
      }
    }

    override def toPersistentRepr(persistenceId: PersistenceId, event: Event[A]): F[PersistentRepr] = {

      def persistentRepr(repr: PersistentRepresentation) = {
        PersistentRepr(
          payload       = repr.payload,
          sequenceNr    = event.seqNr.value,
          persistenceId = persistenceId,
          manifest      = repr.manifest getOrElse PersistentRepr.Undefined,
          writerUuid    = repr.writerUuid,
        )
      }

      val payload = event
        .payload
        .fold {
          s"Event.payload is not defined".fail[F, A]
        } {
          _.pure[F]
        }

      val result = for {
        payload           <- payload
        persistentPayload <- fromEventPayload(payload)
      } yield persistentRepr(persistentPayload)

      result.adaptErr {
        case e =>
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
