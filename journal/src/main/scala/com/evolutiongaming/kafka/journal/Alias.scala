package com.evolutiongaming.kafka.journal

object Alias {
  type Timestamp = Long
  type PersistenceId = String
  type Id = String
  type SeqNr = Long
  type Bytes = Array[Byte]
  type Tag = String
  type Tags = Set[String]
}
