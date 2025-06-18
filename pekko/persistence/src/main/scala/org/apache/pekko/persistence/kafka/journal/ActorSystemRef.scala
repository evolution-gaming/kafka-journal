package org.apache.pekko.persistence.kafka.journal

import cats.~>
import com.evolutiongaming.catshelper.FromFuture
import org.apache.pekko.actor.{ExtendedActorSystem, Extension}

import scala.concurrent.{Future, Promise}
import scala.util.Try

trait ActorSystemRef[F[_], A] extends Extension {

  def get: F[A]

  def set(a: A): F[Unit]
}

object ActorSystemRef {

  trait ExtensionId[A] extends org.apache.pekko.actor.ExtensionId[ActorSystemRef[Future, A]] {

    def createExtension(system: ExtendedActorSystem): ActorSystemRef[Future, A] = {

      val promise = Promise[A]()

      new ActorSystemRef[Future, A] {

        def get = promise.future

        def set(a: A) = Future.fromTry { Try { promise.success(a) } }
      }
    }
  }

  implicit class ActorSystemRefOps[F[_], A](val self: ActorSystemRef[F, A]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): ActorSystemRef[G, A] = new ActorSystemRef[G, A] {

      def get = f(self.get)

      def set(a: A) = f(self.set(a))
    }
  }

  implicit class ActorSystemPromiseFutureOps[A](val self: ActorSystemRef[Future, A]) extends AnyVal {

    def fromFuture[F[_]: FromFuture]: ActorSystemRef[F, A] = new ActorSystemRef[F, A] {

      def get = FromFuture[F].apply { self.get }

      def set(a: A) = FromFuture[F].apply { self.set(a) }
    }
  }
}
