package org.apache.pekko.persistence.kafka.journal

import com.evolutiongaming.kafka.journal.{Headers, RecordMetadata}

final case class AppendMetadata(metadata: RecordMetadata = RecordMetadata.empty, headers: Headers = Headers.empty)

object AppendMetadata {
  val empty: AppendMetadata = AppendMetadata()
}
