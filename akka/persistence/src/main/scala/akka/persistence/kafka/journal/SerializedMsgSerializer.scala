package akka.persistence.kafka.journal

import akka.actor.ActorSystem
import cats.effect.Sync
import cats.syntax.all.*
import cats.~>
import com.evolutiongaming.serialization.{SerializedMsg, SerializedMsgConverter, SerializedMsgExt}

/**
 * Provides ability to convert an object to [[SerializedMsg]] and back.
 *
 * It is a typesafe wrapper over [[SerializedMsgConverter]]
 */
trait SerializedMsgSerializer[F[_]] {

  def toMsg(a: AnyRef): F[SerializedMsg]

  def fromMsg(a: SerializedMsg): F[AnyRef]
}

object SerializedMsgSerializer {

  /**
   * Create [[SerializedMsgSerializer]] from an actor system using [[SerializedMsgExt]]
   */
  def of[F[_]: Sync](actorSystem: ActorSystem): F[SerializedMsgSerializer[F]] = {
    for {
      converter <- Sync[F].delay { SerializedMsgExt(actorSystem) }
    } yield {
      apply(converter)
    }
  }

  def apply[F[_]: Sync](converter: SerializedMsgConverter): SerializedMsgSerializer[F] = {

    new SerializedMsgSerializer[F] {

      def toMsg(a: AnyRef) = {
        Sync[F].delay { converter.toMsg(a) }
      }

      def fromMsg(a: SerializedMsg) = {
        for {
          a <- Sync[F].delay { converter.fromMsg(a) }
          a <- Sync[F].fromTry(a)
        } yield a
      }
    }
  }

  implicit class SerializedMsgSerializerOps[F[_]](val self: SerializedMsgSerializer[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): SerializedMsgSerializer[G] = new SerializedMsgSerializer[G] {

      def toMsg(a: AnyRef) = f(self.toMsg(a))

      def fromMsg(a: SerializedMsg) = f(self.fromMsg(a))
    }
  }
}
