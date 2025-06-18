package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait ActorSuite extends BeforeAndAfterAll { self: Suite =>

  implicit lazy val actorSystem: ActorSystem = ActorSystem(getClass.getSimpleName, configOf())

  def configOf(): Config = ConfigFactory.load()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val _ = actorSystem
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(actorSystem)
    super.afterAll()
  }

  abstract class ActorScope extends TestKit(actorSystem) with ImplicitSender with DefaultTimeout
}
