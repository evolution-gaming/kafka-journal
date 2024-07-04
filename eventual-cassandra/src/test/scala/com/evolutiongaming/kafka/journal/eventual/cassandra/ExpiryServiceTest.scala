package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.Poll
import cats.effect.kernel.CancelScope
import cats.syntax.all.*
import cats.{Id, Monad, catsInstancesForId}
import com.evolutiongaming.kafka.journal.ExpireAfter
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits.*
import com.evolutiongaming.kafka.journal.eventual.cassandra.ExpireOn.implicits.*
import com.evolutiongaming.kafka.journal.eventual.cassandra.ExpiryService.Action
import com.evolutiongaming.kafka.journal.util.MonadCancelFromMonad
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.{Instant, LocalDate, ZoneOffset}
import scala.concurrent.duration.*
import scala.util.Try
import scala.util.control.NonFatal

class ExpiryServiceTest extends AnyFunSuite with Matchers {
  import ExpiryServiceTest.*

  test("expireOn") {
    val expireAfter = 1.day.toExpireAfter
    val expected    = LocalDate.of(2019, 12, 12).toExpireOn
    expireService.expireOn(expireAfter, timestamp) shouldEqual expected
  }

  for {
    (expiry, expireAfter, action) <- List(
      (
        none[Expiry],
        1.minute.toExpireAfter.some,
        Action.update(Expiry(1.minute.toExpireAfter, LocalDate.of(2019, 12, 11).toExpireOn)),
      ),
      (none[Expiry], 1.day.toExpireAfter.some, Action.update(Expiry(1.day.toExpireAfter, LocalDate.of(2019, 12, 12).toExpireOn))),
      (
        Expiry(1.day.toExpireAfter, LocalDate.of(2019, 12, 11).toExpireOn).some,
        1.day.toExpireAfter.some,
        Action.update(Expiry(1.day.toExpireAfter, LocalDate.of(2019, 12, 12).toExpireOn)),
      ),
      (Expiry(1.day.toExpireAfter, LocalDate.of(2019, 12, 12).toExpireOn).some, 1.day.toExpireAfter.some, Action.ignore),
      (Expiry(1.day.toExpireAfter, LocalDate.of(2019, 12, 12).toExpireOn).some, none[ExpireAfter], Action.remove),
    )
  } yield {
    test(s"action expiry: $expiry, expireAfter: $expireAfter, action: $action") {
      expireService.action(expiry, expireAfter, timestamp) shouldEqual action
    }
  }
}

object ExpiryServiceTest {

  implicit val bracketId: MonadCancelFromMonad[Id, Throwable] = new MonadCancelFromMonad[Id, Throwable] {

    def F: Monad[Id] = catsInstancesForId

    override def rootCancelScope: CancelScope = CancelScope.Uncancelable

    override def forceR[A, B](fa: Id[A])(fb: Id[B]): Id[B] = Try(fa).fold(_ => fb, _ => fb)

    override def uncancelable[A](body: Poll[Id] => Id[A]): Id[A] = body(new Poll[Id] {
      override def apply[X](fa: Id[X]): Id[X] = fa
    })

    override def canceled: Id[Unit] = ().pure[Id]

    override def onCancel[A](fa: Id[A], fin: Id[Unit]): Id[A] = fa

    override def raiseError[A](e: Throwable): Id[A] = throw e

    override def handleErrorWith[A](fa: Id[A])(f: Throwable => Id[A]): Id[A] = try { fa }
    catch { case NonFatal(e) => f(e) }
  }

  val timestamp: Instant = Instant.parse("2019-12-11T10:10:10.00Z")

  val zoneId: ZoneOffset = ZoneOffset.UTC

  val expireService: ExpiryService[Id] = ExpiryService[Id](zoneId)
}
