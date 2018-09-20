package com.evolutiongaming.hostname

import org.scalatest.{FunSuite, Matchers}

class HostNameSpec extends FunSuite with Matchers {

  val hostName = HostName()

  val name = hostName.fold("undefined")(_.toString)

  test(s"apply at $name") {
    hostName.isDefined shouldEqual true
  }

  test("inetAddress") {
    HostName.inetAddress().isDefined shouldEqual true
  }

  test("win or unit") {
    val hostName = HostName.win() orElse HostName.unix()
    hostName.isDefined shouldEqual true
  }
}