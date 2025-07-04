package com.evolution.kafka.journal.conversions

final case class ConversionMetrics[F[_]](
  kafkaRead: KafkaReadMetrics[F],
  kafkaWrite: KafkaWriteMetrics[F],
)
