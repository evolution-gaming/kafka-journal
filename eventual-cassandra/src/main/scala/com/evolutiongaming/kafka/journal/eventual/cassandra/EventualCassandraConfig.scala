package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.Config

case class EventualCassandraConfig(
  /*segmentSize: Int = 500000*/
  segmentSize: Int = 10)

object EventualCassandraConfig {

  val Default: EventualCassandraConfig = EventualCassandraConfig()


  def apply(config: Config): EventualCassandraConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    EventualCassandraConfig(
      segmentSize = get[Int]("segment-size") getOrElse Default.segmentSize)
  }
}