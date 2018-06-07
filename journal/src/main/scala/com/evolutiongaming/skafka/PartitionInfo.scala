package com.evolutiongaming.skafka

import org.apache.kafka.common.Node

case class PartitionInfo(
  topic: Topic,
  partition: Partition,
  leader: Node,
  replicas: List[Node],
  inSyncReplicas: List[Node],
  offlineReplicas: List[Node])
