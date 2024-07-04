package com.evolutiongaming.kafka.journal.cassandra

import com.datastax.driver.core.ConsistencyLevel
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

final case class CassandraConsistencyConfig(
  read: CassandraConsistencyConfig.Read   = CassandraConsistencyConfig.Read.default,
  write: CassandraConsistencyConfig.Write = CassandraConsistencyConfig.Write.default,
)

object CassandraConsistencyConfig {

  implicit val configReaderConsistencyConfig: ConfigReader[CassandraConsistencyConfig] = deriveReader

  val default: CassandraConsistencyConfig = CassandraConsistencyConfig()

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
