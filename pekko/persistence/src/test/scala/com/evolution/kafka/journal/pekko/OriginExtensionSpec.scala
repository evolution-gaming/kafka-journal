package com.evolution.kafka.journal.pekko

import cats.effect.IO
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.Origin
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class OriginExtensionSpec extends AsyncFunSuite with Matchers {

  withSystem { system =>
    test("pekkoHost") {
      val result = for {
        pekkoHost <- OriginExtension.pekkoHost[IO](system)
        result = pekkoHost.isDefined shouldEqual false
      } yield result
      result.run()
    }

    test("PekkoName") {
      OriginExtension.pekkoName(system) shouldEqual Origin("OriginSpec")
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
