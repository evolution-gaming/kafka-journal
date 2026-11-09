package com.evolution.kafka.journal.cassandra

import com.datastax.oss.driver.api.core.{ConsistencyLevel, DefaultConsistencyLevel}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

private[journal] final case class CassandraConsistencyConfig(
  read: CassandraConsistencyConfig.Read = CassandraConsistencyConfig.Read.default,
  write: CassandraConsistencyConfig.Write = CassandraConsistencyConfig.Write.default,
)

private[journal] object CassandraConsistencyConfig {

  implicit val configReaderConsistencyConfig: ConfigReader[CassandraConsistencyConfig] = deriveReader

  val default: CassandraConsistencyConfig = CassandraConsistencyConfig()

  // `ConsistencyLevel` of the Java driver 4 is an interface, the built-in levels are read as enum

  final case class Read(value: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM)

  object Read {
    val default: Read = Read()

    implicit val configReaderRead: ConfigReader[Read] = ConfigReader[DefaultConsistencyLevel].map { a => Read(a) }
  }

  final case class Write(value: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM)

  object Write {
    val default: Write = Write()

    implicit val configReaderWrite: ConfigReader[Write] = ConfigReader[DefaultConsistencyLevel].map { a => Write(a) }
  }
}
