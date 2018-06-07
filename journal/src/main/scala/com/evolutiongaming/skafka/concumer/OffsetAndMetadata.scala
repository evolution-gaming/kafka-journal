package com.evolutiongaming.skafka.concumer

import com.evolutiongaming.skafka.Offset

case class OffsetAndMetadata(offset: Offset, metadata: String/*TODO type*/)
