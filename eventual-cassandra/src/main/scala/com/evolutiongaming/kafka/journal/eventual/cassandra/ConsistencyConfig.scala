package com.evolutiongaming.kafka.journal.eventual.cassandra

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import com.datastax.driver.core.ConsistencyLevel

final case class ConsistencyConfig(
  read: ConsistencyConfig.Read = ConsistencyConfig.Read.default,
  write: ConsistencyConfig.Write = ConsistencyConfig.Write.default
)

object ConsistencyConfig {

  implicit val configReaderConsistencyConfig: ConfigReader[ConsistencyConfig] = deriveReader

  val default: ConsistencyConfig = ConsistencyConfig()

  final case class Read(value: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM)

  object Read {
    val default: Read = Read()

    implicit val configReaderRead: ConfigReader[Read] = ConfigReader[ConsistencyLevel].map { a => Read(a) }
  }

  final case class Write(value: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM)

  object Write {
    val default: Write = Write()

    implicit val configReaderWrite: ConfigReader[Write] = ConfigReader[ConsistencyLevel].map { a => Write(a) }
  }
}
