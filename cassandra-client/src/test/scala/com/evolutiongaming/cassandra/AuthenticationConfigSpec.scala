package com.evolutiongaming.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

class AuthenticationConfigSpec extends FunSuite with Matchers {

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("authentication.conf"))
    val expected = AuthenticationConfig(
      username = "username",
      password = "password")
    AuthenticationConfig(config) shouldEqual expected
  }
}