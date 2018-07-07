package com.evolutiongaming.kafka.journal

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId}

trait ClientExtension extends Extension {
  def client: Journal
}

object ClientExtension extends ExtensionId[ClientExtension] {

  def createExtension(system: ExtendedActorSystem): ClientExtension = {

    val settings = Settings(system.settings.config)

    new ClientExtension {
      val client = Journal(settings)
    }
  }
}
