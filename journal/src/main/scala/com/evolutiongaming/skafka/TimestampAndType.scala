package com.evolutiongaming.skafka


case class TimestampAndType(timestamp: Timestamp, timestampType: TimestampType)

sealed trait TimestampType

object TimestampType {
  case object Create extends TimestampType
  case object Append extends TimestampType
}