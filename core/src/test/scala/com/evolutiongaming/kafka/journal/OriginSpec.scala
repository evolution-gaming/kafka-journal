package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import com.evolutiongaming.kafka.journal.IOSuite.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class OriginSpec extends AsyncFunSuite with Matchers {

  test("hostName") {
    val result = for {
      hostName <- Origin.hostName[IO]
      result    = hostName.isDefined shouldEqual true
    } yield result
    result.run()
  }

  withSystem { system =>
    test("akkaHost") {
      val result = for {
        akkaHost <- Origin.akkaHost[IO](system)
        result    = akkaHost.isDefined shouldEqual false
      } yield result
      result.run()
    }

    test("AkkaName") {
      Origin.akkaName(system) shouldEqual Origin("OriginSpec")
    }
  }

  private def withSystem[T](f: ActorSystem => T): T = {
    val system = ActorSystem("OriginSpec")
    try {
      f(system)
    } finally {
      TestKit.shutdownActorSystem(system, 3.seconds)
    }
  }
}
