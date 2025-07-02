package com.evolution.kafka.journal.eventual.cassandra

import com.evolution.kafka.journal.Headers
import com.evolution.kafka.journal.eventual.cassandra.CassandraHelper.*
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}

private[journal] object HeadersHelper {

  implicit val headersEncodeRow: EncodeRow[Headers] = EncodeRow("headers")

  implicit val headersDecodeRow: DecodeRow[Headers] = DecodeRow("headers")
}
