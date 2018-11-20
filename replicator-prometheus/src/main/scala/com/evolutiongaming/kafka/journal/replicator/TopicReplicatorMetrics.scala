package com.evolutiongaming.kafka.journal.replicator

import com.evolutiongaming.kafka.journal.IO
import com.evolutiongaming.kafka.journal.replicator.MetricsHelper._
import com.evolutiongaming.kafka.journal.replicator.TopicReplicator.Metrics.Measurements
import com.evolutiongaming.skafka.Topic
import io.prometheus.client.{CollectorRegistry, Summary}

object TopicReplicatorMetrics {

  def apply[F[_]: IO](
    registry: CollectorRegistry,
    prefix: String = "replicator"): Topic => TopicReplicator.Metrics[F] = {

    val replicationSummary = Summary.build()
      .name(s"${ prefix }_replication_latency")
      .help("Replication latency in seconds")
      .labelNames("topic", "partition", "type")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.05)
      .quantile(0.95, 0.01)
      .quantile(0.99, 0.005)
      .register(registry)

    val deliverySummary = Summary.build()
      .name(s"${ prefix }_delivery_latency")
      .help("Delivery latency in seconds")
      .labelNames("topic", "partition", "type")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.05)
      .quantile(0.95, 0.01)
      .quantile(0.99, 0.005)
      .register(registry)

    val eventsSummary = Summary.build()
      .name(s"${ prefix }_events")
      .help("Number of events replicated")
      .labelNames("topic", "partition")
      .register(registry)

    val bytesSummary = Summary.build()
      .name(s"${ prefix }_bytes")
      .help("Number of bytes replicated")
      .labelNames("topic", "partition")
      .register(registry)

    val recordsSummary = Summary.build()
      .name(s"${ prefix }_records")
      .help("Number of kafka records processed")
      .labelNames("topic", "partition")
      .register(registry)

    val roundSummary = Summary.build()
      .name(s"${ prefix }_round_duration")
      .help("Replication round duration")
      .labelNames("topic")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.05)
      .quantile(0.95, 0.01)
      .quantile(0.99, 0.005)
      .register(registry)

    val roundRecordsSummary = Summary.build()
      .name(s"${ prefix }_round_records")
      .help("Number of kafka records processed in round")
      .labelNames("topic")
      .register(registry)

    topic: Topic => {

      def observeMeasurements(name: String, measurements: Measurements) = {

        val partition = measurements.partition.toString

        replicationSummary
          .labels(topic, partition, name)
          .observe(measurements.replicationLatency.toSeconds)

        deliverySummary
          .labels(topic, partition, name)
          .observe(measurements.deliveryLatency.toSeconds)

        recordsSummary
          .labels(topic, partition)
          .observe(measurements.records.toDouble)
      }

      new TopicReplicator.Metrics[F] {

        def append(events: Int, bytes: Int, measurements: Measurements) = {
          IO[F].effect {
            val partition = measurements.partition.toString

            observeMeasurements("append", measurements)

            eventsSummary
              .labels(topic, partition)
              .observe(events.toDouble)

            bytesSummary
              .labels(topic, partition)
              .observe(bytes.toDouble)
          }
        }

        def delete(measurements: Measurements) = {
          IO[F].effect {
            observeMeasurements("delete", measurements)
          }
        }

        def round(duration: Long, records: Int) = {
          IO[F].effect {
            roundSummary
              .labels(topic)
              .observe(duration.toSeconds)

            roundRecordsSummary
              .labels(topic)
              .observe(records.toDouble)
          }
        }
      }
    }
  }
}
