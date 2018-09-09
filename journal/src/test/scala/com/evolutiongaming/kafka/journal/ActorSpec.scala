package com.evolutiongaming.kafka.journal


import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait ActorSpec extends BeforeAndAfterAll {
  this: Suite =>

  implicit lazy val system: ActorSystem = ActorSystem(getClass.getSimpleName, configOf())

  def configOf(): Config = ConfigFactory.load()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    system
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  abstract class ActorScope extends TestKit(system) with ImplicitSender with DefaultTimeout
}
