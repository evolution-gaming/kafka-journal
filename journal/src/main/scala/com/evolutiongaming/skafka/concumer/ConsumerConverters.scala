package com.evolutiongaming.skafka.concumer

import java.util.{Map => MapJ}

import com.evolutiongaming.skafka.Converters._
import com.evolutiongaming.skafka.Converters2._
import com.evolutiongaming.skafka.{TimestampAndType, TimestampType}
import org.apache.kafka.clients.consumer.{ConsumerRebalanceListener => RebalanceListenerJ, ConsumerRecord => ConsumerRecordJ, ConsumerRecords => ConsumerRecordsJ, OffsetAndMetadata => OffsetAndMetadataJ, OffsetAndTimestamp => OffsetAndTimestampJ, OffsetCommitCallback => CommitCallbackJ}
import org.apache.kafka.common.record.{TimestampType => TimestampTypeJ}
import org.apache.kafka.common.{TopicPartition => TopicPartitionJ}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

object ConsumerConverters {

  implicit class OffsetAndMetadataJOps(val self: OffsetAndMetadataJ) extends AnyVal {
    def asScala: OffsetAndMetadata = OffsetAndMetadata(self.offset(), self.metadata())
  }


  implicit class OffsetAndMetadataOps(val self: OffsetAndMetadata) extends AnyVal {
    def asJava: OffsetAndMetadataJ = new OffsetAndMetadataJ(self.offset, self.metadata)
  }


  implicit class OffsetAndTimestampJOps(val self: OffsetAndTimestampJ) extends AnyVal {
    def asScala: OffsetAndTimestamp = OffsetAndTimestamp(offset = self.offset(), timestamp = self.timestamp())
  }


  implicit class OffsetAndTimestampOps(val self: OffsetAndTimestamp) extends AnyVal {
    def asJava: OffsetAndTimestampJ = new OffsetAndTimestampJ(self.offset, self.timestamp)
  }


  implicit class RebalanceListenerOps(val self: RebalanceListener) extends AnyVal {

    def asJava: RebalanceListenerJ = new RebalanceListenerJ {

      def onPartitionsAssigned(partitions: java.util.Collection[TopicPartitionJ]) = {
        val partitionsS = partitions.asScala.map(_.asScala)
        self.onPartitionsAssigned(partitionsS)
      }

      def onPartitionsRevoked(partitions: java.util.Collection[TopicPartitionJ]) = {
        val partitionsS = partitions.asScala.map(_.asScala)
        self.onPartitionsRevoked(partitionsS)
      }
    }
  }


  implicit class CommitCallbackOps(val self: CommitCallback) extends AnyVal {

    def asJava: CommitCallbackJ = new CommitCallbackJ {

      def onComplete(offsetsJ: MapJ[TopicPartitionJ, OffsetAndMetadataJ], exception: Exception): Unit = {
        val offsets =
          if (exception == null) {
            val offsets = offsetsJ.asScalaMap(_.asScala, _.asScala)
            Success(offsets)
          } else {
            Failure(exception)
          }
        self(offsets)
      }
    }
  }


  implicit class ConsumerRecordJOps[K, V](val self: ConsumerRecordJ[K, V]) extends AnyVal {

    def asScala: ConsumerRecord[K, V] = {

      val headers = self.headers().asScala.map(_.asScala).toList

      val timestampAndType = {
        def some(timestampType: TimestampType) = {
          Some(TimestampAndType(self.timestamp(), timestampType))
        }

        self.timestampType() match {
          case TimestampTypeJ.NO_TIMESTAMP_TYPE => None
          case TimestampTypeJ.CREATE_TIME       => some(TimestampType.Create)
          case TimestampTypeJ.LOG_APPEND_TIME   => some(TimestampType.Append)
        }
      }
      ConsumerRecord(
        topic = self.topic(),
        partition = self.partition(),
        offset = self.offset(),
        timestampAndType = timestampAndType,
        serializedKeySize = self.serializedKeySize(),
        serializedValueSize = self.serializedValueSize(),
        key = Option(self.key()),
        value = self.value(),
        headers = headers)
    }
  }


  implicit class ConsumerRecordsJOps[K, V](val self: ConsumerRecordsJ[K, V]) extends AnyVal {

    def asScala: ConsumerRecords[K, V] = {
      val partitions = self.partitions()
      val records = for {
        partitionJ <- partitions.asScala
      } yield {
        val recordsJ = self.records(partitionJ)
        val partition = partitionJ.asScala
        val records = recordsJ.asScala.map(_.asScala).toVector
        (partition, records)
      }
      ConsumerRecords(records.toMap)
    }
  }
}
