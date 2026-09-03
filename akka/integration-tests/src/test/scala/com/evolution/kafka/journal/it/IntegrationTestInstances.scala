package com.evolution.kafka.journal.it

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.evolution.kafka.journal.*
import com.evolutiongaming.catshelper.{LogOf, RandomIdOf}
import com.evolutiongaming.scassandra.CassandraClusterOf

import scala.util.Try

/**
 * Combines typeclass instances used in integration tests to reduce code duplication.
 *
 * It is assumed that the integration tests use [[IO]] directly.
 *
 * Only "lightweight" instances are put here - the ones which do not require special disposal logic.
 */
private[it] object IntegrationTestInstances {
  implicit val ioRuntime: IORuntime = cats.effect.unsafe.implicits.global
  implicit val logOf: LogOf[IO] = LogOf.slf4j[IO].unsafeRunSync()
  implicit val cassandraClusterOf: CassandraClusterOf[IO] = CassandraClusterOf.of[IO].unsafeRunSync()
  implicit val kafkaConsumerOf: KafkaConsumerOf[IO] = KafkaConsumerOf()
  implicit val kafkaProducerOf: KafkaProducerOf[IO] = KafkaProducerOf()
  implicit val randomIdOf: RandomIdOf[IO] = RandomIdOf.uuid
  implicit val jsonCodecIo: JsonCodec[IO] = JsonCodec.default
  implicit val jsonCodecTry: JsonCodec[Try] = JsonCodec.default
}
