package com.evolutiongaming.kafka.journal.cassandra

import com.datastax.driver.core.ConsistencyLevel
import pureconfig.{ConfigCursor, ConfigReader}

final case class CassandraConsistencyConfig(
  read: CassandraConsistencyConfig.Read   = CassandraConsistencyConfig.Read.default,
  write: CassandraConsistencyConfig.Write = CassandraConsistencyConfig.Write.default,
)

object CassandraConsistencyConfig {

  implicit val configReaderConsistencyConfig: ConfigReader[CassandraConsistencyConfig] = new ConfigReader[CassandraConsistencyConfig] {
    override def from(cur: ConfigCursor): ConfigReader.Result[CassandraConsistencyConfig] = {
      for {
        objCur <- cur.asObjectCursor
        readCur = objCur.atKeyOrUndefined("read")
        read <- Read.configReaderRead.from(readCur)
        writeCur = objCur.atKeyOrUndefined("write")
        write = if (writeCur.isUndefined) CassandraConsistencyConfig.Write.default else (Write.configReaderWrite.from(writeCur))
      } yield CassandraConsistencyConfig(read, write)
    }
  }

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
