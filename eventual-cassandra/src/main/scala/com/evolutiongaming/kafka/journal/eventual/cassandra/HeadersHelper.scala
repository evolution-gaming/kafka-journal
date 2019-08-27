package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.Headers
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}

object HeadersHelper {

  implicit val HeadersEncodeRow: EncodeRow[Headers] = EncodeRow("headers")

  implicit val HeadersDecodeRow: DecodeRow[Headers] = DecodeRow("headers")
}
