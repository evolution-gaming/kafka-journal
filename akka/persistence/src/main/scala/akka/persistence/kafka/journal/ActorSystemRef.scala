package akka.persistence.kafka.journal

import akka.actor.{ExtendedActorSystem, Extension}
import cats.~>
import com.evolutiongaming.catshelper.FromFuture

import scala.concurrent.{Future, Promise}
import scala.util.Try

trait ActorSystemRef[F[_], A] extends Extension {

  def get: F[A]

  def set(a: A): F[Unit]
}

object ActorSystemRef {

  trait ExtensionId[A] extends akka.actor.ExtensionId[ActorSystemRef[Future, A]] {

    def createExtension(system: ExtendedActorSystem): ActorSystemRef[Future, A] = {

      val promise = Promise[A]()

      new ActorSystemRef[Future, A] {

        def get: Future[A] = promise.future

        def set(a: A): Future[Unit] = Future.fromTry { Try { promise.success(a) } }
      }
    }
  }

  implicit class ActorSystemRefOps[F[_], A](val self: ActorSystemRef[F, A]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): ActorSystemRef[G, A] = new ActorSystemRef[G, A] {

      def get: G[A] = f(self.get)

      def set(a: A): G[Unit] = f(self.set(a))
    }
  }

  implicit class ActorSystemPromiseFutureOps[A](val self: ActorSystemRef[Future, A]) extends AnyVal {

    def fromFuture[F[_]: FromFuture]: ActorSystemRef[F, A] = new ActorSystemRef[F, A] {

      def get: F[A] = FromFuture[F].apply { self.get }

      def set(a: A): F[Unit] = FromFuture[F].apply { self.set(a) }
    }
  }
}
