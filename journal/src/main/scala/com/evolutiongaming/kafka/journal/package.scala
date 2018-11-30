package com.evolutiongaming.kafka

package object journal {

  // TODO rename to not conflict with cats
  type Id = String

  type Bytes = Array[Byte]

  type Tag = String

  type Tags = Set[Tag]
}
