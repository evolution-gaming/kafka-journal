package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.{Instant, LocalDate, ZoneOffset}

import cats.effect.ExitCase
import cats.syntax.all._
import cats.{Id, catsInstancesForId}
import com.evolutiongaming.kafka.journal.ExpireAfter
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits._
import com.evolutiongaming.kafka.journal.eventual.cassandra.ExpireOn.implicits._
import com.evolutiongaming.kafka.journal.eventual.cassandra.ExpiryService.Action
import com.evolutiongaming.kafka.journal.util.BracketFromMonad
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.util.control.NonFatal
import org.scalatest.funsuite.AnyFunSuite

class ExpiryServiceTest extends AnyFunSuite with Matchers {
  import ExpiryServiceTest._

  test("expireOn") {
    val expireAfter = 1.day.toExpireAfter
    val expected = LocalDate.of(2019, 12, 12).toExpireOn
    expireService.expireOn(expireAfter, timestamp) shouldEqual expected
  }

  for {
    (expiry, expireAfter, action) <- List(
      (
        none[Expiry],
        1.minute.toExpireAfter.some,
        Action.update(Expiry(
          1.minute.toExpireAfter,
          LocalDate.of(2019, 12, 11).toExpireOn))),
      (
        none[Expiry],
        1.day.toExpireAfter.some,
        Action.update(Expiry(
          1.day.toExpireAfter,
          LocalDate.of(2019, 12, 12).toExpireOn))),
      (
        Expiry(
          1.day.toExpireAfter,
          LocalDate.of(2019, 12, 11).toExpireOn).some,
        1.day.toExpireAfter.some,
        Action.update(Expiry(
          1.day.toExpireAfter,
          LocalDate.of(2019, 12, 12).toExpireOn))),
      (
        Expiry(
          1.day.toExpireAfter,
          LocalDate.of(2019, 12, 12).toExpireOn).some,
        1.day.toExpireAfter.some,
        Action.ignore),
      (
        Expiry(
          1.day.toExpireAfter,
          LocalDate.of(2019, 12, 12).toExpireOn).some,
        none[ExpireAfter],
        Action.remove))
  } yield {
    test(s"action expiry: $expiry, expireAfter: $expireAfter, action: $action") {
      expireService.action(expiry, expireAfter, timestamp) shouldEqual action
    }
  }
}

object ExpiryServiceTest {

  implicit val bracketId: BracketFromMonad[Id, Throwable] = new BracketFromMonad[Id, Throwable] {

    def F = catsInstancesForId

    def bracketCase[A, B](acquire: Id[A])(use: A => Id[B])(release: (A, ExitCase[Throwable]) => Id[Unit]) = {
      flatMap(acquire) { a =>
        try {
          val b = use(a)
          try release(a, ExitCase.Completed) catch { case NonFatal(_) => }
          b
        } catch {
          case NonFatal(e) =>
            release(a, ExitCase.Error(e))
            raiseError(e)
        }
      }
    }

    def raiseError[A](a: Throwable) = throw a

    def handleErrorWith[A](fa: Id[A])(f: Throwable => Id[A]) = fa
  }

  val timestamp: Instant = Instant.parse("2019-12-11T10:10:10.00Z")

  val zoneId: ZoneOffset = ZoneOffset.UTC

  val expireService: ExpiryService[Id] = ExpiryService[Id](zoneId)
}
