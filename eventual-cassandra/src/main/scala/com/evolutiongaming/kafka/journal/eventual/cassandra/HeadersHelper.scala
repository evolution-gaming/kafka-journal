package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.Headers
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper.*
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}

object HeadersHelper {

  implicit val headersEncodeRow: EncodeRow[Headers] = EncodeRow("headers")

  implicit val headersDecodeRow: DecodeRow[Headers] = DecodeRow("headers")
}
