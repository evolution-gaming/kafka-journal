package com.evolutiongaming.cassandra

import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.Config

/**
  * See [[https://docs.datastax.com/en/developer/java-driver/3.5/manual/auth/]]
  */
final case class AuthenticationConfig(username: String, password: String)

object AuthenticationConfig {

  def apply(config: Config): AuthenticationConfig = {
    AuthenticationConfig(
      username = config.get[String]("username"),
      password = config.get[String]("password"))
  }
}
