package com.evolutiongaming.kafka.journal

object Alias {

  type PersistenceId = String
  
  type Id = String

  type Tag = String


  type Tags = Set[String]

  object Tags {
    val Empty: Tags = Set.empty
  }
}
