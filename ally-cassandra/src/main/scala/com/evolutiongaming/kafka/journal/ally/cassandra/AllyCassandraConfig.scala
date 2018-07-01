package com.evolutiongaming.kafka.journal.ally.cassandra

import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.Config

case class AllyCassandraConfig(
  /*segmentSize: Int = 500000*/
  segmentSize: Int = 100)

object AllyCassandraConfig {

  val Default: AllyCassandraConfig = AllyCassandraConfig()


  def apply(config: Config): AllyCassandraConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    AllyCassandraConfig(
      segmentSize = get[Int]("segment-size") getOrElse Default.segmentSize)
  }
}