package com.evolutiongaming.cassandra

import com.evolutiongaming.config.ConfigHelper.FromConf
import com.evolutiongaming.nel.Nel
import com.typesafe.config.ConfigException

object ConfigHelpers {

  // TODO copied from com.evolutiongaming.skafka.producer.ProducerConfig
  implicit def nelFromConf[T](implicit fromConf: FromConf[List[T]]): FromConf[Nel[T]] = {
    FromConf { case (conf, path) =>
      val list = fromConf(conf, path)
      list match {
        case Nil     => throw new ConfigException.BadValue(conf.origin(), path, s"Cannot parse Nel from empty list")
        case x :: xs => Nel(x, xs)
      }
    }
  }
}
