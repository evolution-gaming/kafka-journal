package com.evolutiongaming.cassandra

import org.apache.cassandra.config.{Config, ConfigurationLoader}

import scala.util.DynamicVariable

class ConfigLoader extends ConfigurationLoader {

  def loadConfig(): Config = {
    ConfigLoader.config getOrElse sys.error("config is not defined")
  }
}

object ConfigLoader {
  private val variable = new DynamicVariable(Option.empty[Config])

  def config: Option[Config] = variable.value
  def config_=(config: Config): Unit = variable.value = Some(config)
}