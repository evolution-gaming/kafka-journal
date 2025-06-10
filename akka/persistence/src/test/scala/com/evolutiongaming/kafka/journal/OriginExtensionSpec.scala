package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.effect.IO
import com.evolutiongaming.kafka.journal.IOSuite.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class OriginExtensionSpec extends AsyncFunSuite with Matchers {

  withSystem { system =>
    test("akkaHost") {
      val result = for {
        akkaHost <- OriginExtension.akkaHost[IO](system)
        result = akkaHost.isDefined shouldEqual false
      } yield result
      result.run()
    }

    test("AkkaName") {
      OriginExtension.akkaName(system) shouldEqual Origin("OriginSpec")
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
