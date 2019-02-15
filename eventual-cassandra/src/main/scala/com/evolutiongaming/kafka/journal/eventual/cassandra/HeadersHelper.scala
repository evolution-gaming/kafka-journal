package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.kafka.journal.Headers
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}

object HeadersHelper {

  implicit val HeadersEncodeRow: EncodeRow[Headers] = new EncodeRow[Headers] {
    def apply[B <: SettableData[B]](data: B, value: Headers) = {
      data.encode("headers", value)
    }
  }

  implicit val HeadersDecodeRow: DecodeRow[Headers] = new DecodeRow[Headers] {
    def apply(data: GettableByNameData) = {
      data.decode[Headers]("headers")
    }
  }
}
