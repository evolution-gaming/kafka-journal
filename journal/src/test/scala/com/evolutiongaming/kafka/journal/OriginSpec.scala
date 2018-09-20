package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class OriginSpec extends FunSuite with Matchers {

  test("HostName") {
    Origin.HostName.isDefined shouldEqual true
  }

  withSystem { system =>
    test("AkkaHost") {
      val origin = Origin.AkkaHost(system)
      origin.isDefined shouldEqual false
    }

    test("AkkaName") {
      Origin.AkkaName(system) shouldEqual Origin("OriginSpec")
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
