package com.evolutiongaming.skafka.concumer

import com.evolutiongaming.skafka.{Offset, Timestamp}

case class OffsetAndTimestamp(offset: Offset, timestamp: Timestamp)
