package com.evolutiongaming.skafka

import java.util.{Map => MapJ}

import org.apache.kafka.common.{PartitionInfo => PartitionInfoJ, TopicPartition => TopicPartitionJ}

import scala.collection.JavaConverters._

object Converters2 {

  implicit class TopicPartitionOps(val self: TopicPartition) extends AnyVal {
    def asJava: TopicPartitionJ = new TopicPartitionJ(self.topic, self.partition)
  }


  implicit class TopicPartitionJOps(val self: TopicPartitionJ) extends AnyVal {
    def asScala: TopicPartition = TopicPartition(self.topic(), self.partition())
  }


  implicit class PartitionInfoJOps(val self: PartitionInfoJ) extends AnyVal {
    
    def asScala: PartitionInfo = {
      PartitionInfo(
        topic = self.topic,
        partition = self.partition,
        leader = self.leader(),
        replicas = self.replicas.toList,
        inSyncReplicas = self.inSyncReplicas.toList,
        offlineReplicas = self.offlineReplicas.toList)
    }
  }


  implicit class MapJOps[K, V](val self: MapJ[K, V]) extends AnyVal {

    def asScalaMap[KK, VV](kf: K => KK, vf: V => VV): Map[KK, VV] = {
      val zero = Map.empty[KK, VV]
      self.asScala.foldLeft(zero) { case (map, (k, v)) =>
        val kk = kf(k)
        val vv = vf(v)
        map.updated(kk, vv)
      }
    }
  }


  implicit class MapOps[K, V](val self: Map[K, V]) extends AnyVal {

    def asJavaMap[KK, VV](kf: K => KK, vf: V => VV): MapJ[KK, VV] = {
      val zero = new java.util.HashMap[KK, VV]()
      self.foldLeft(zero) { case (map, (k, v)) =>
        val kk = kf(k)
        val vv = vf(v)
        map.put(kk, vv)
        map
      }
    }
  }
}
