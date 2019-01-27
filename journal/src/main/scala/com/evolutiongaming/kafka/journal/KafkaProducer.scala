package com.evolutiongaming.kafka.journal

import cats.effect._
import cats.implicits._
import cats.{Applicative, ~>}
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.producer.ProducerConverters._
import com.evolutiongaming.skafka.producer._
import org.apache.kafka.clients.producer.{Callback, Producer => ProducerJ, RecordMetadata => RecordMetadataJ}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait KafkaProducer[F[_]] {

  def send[K: skafka.ToBytes, V: skafka.ToBytes](record: ProducerRecord[K, V]): F[RecordMetadata]

  def flush: F[Unit]
}

object KafkaProducer {

  def apply[F[_]](implicit F: KafkaProducer[F]): KafkaProducer[F] = F

  def of[F[_] : Async : ContextShift : Clock](
    config: ProducerConfig,
    blocking: ExecutionContext,
    metrics: Option[KafkaProducer.Metrics[F]] = None
  ): Resource[F, KafkaProducer[F]] = {

    val blocking1 = new (F ~> F) {
      def apply[A](fa: F[A]) = ContextShift[F].evalOn(blocking)(fa)
    }

    val result = for {
      producer <- blocking1 { Sync[F].delay { CreateJProducer(config) } }
    } yield {
      val producer1 = apply(producer).mapK(blocking1)
      val close = blocking1 { Sync[F].delay { producer.close() } }

      val (producer2, close1) = metrics.fold((producer1, close)) { metrics =>
        val producer2 = producer1.withMetrics(metrics)
        val close1 = for {
          ab     <- Latency { close }
          (r, l)  = ab
          _      <- metrics.close(l)
        } yield r
        (producer2, close1)
      }

      val release = for {
        _ <- producer2.flush
        _ <- close1
      } yield {}

      (producer2, release)
    }

    Resource(result)
  }


  def apply[F[_] : Async](producer: ProducerJ[Bytes, Bytes]): KafkaProducer[F] = {
    new KafkaProducer[F] {

      def send[K: skafka.ToBytes, V: skafka.ToBytes](record: ProducerRecord[K, V]) = {
        val recordBytes = record.toBytes.asJava

        val send = Async[F].async[RecordMetadataJ] { callback =>

          val callback1 = new Callback {
            def onCompletion(metadata: RecordMetadataJ, failure: Exception) = {
              val result = {
                if (failure != null) {
                  failure.asLeft[RecordMetadataJ]
                } else if (metadata != null) {
                  metadata.asRight[Throwable]
                } else {
                  val failure = new RuntimeException("both metadata & exception are nulls")
                  failure.asLeft[RecordMetadataJ]
                }
              }
              callback(result)
            }
          }

          try {
            val _ = producer.send(recordBytes, callback1)
          } catch {
            case NonFatal(failure) => callback(failure.asLeft[RecordMetadataJ])
          }
        }

        for {
          metadata <- send
        } yield {
          metadata.asScala
        }
      }

      def flush = {
        Sync[F].delay { producer.flush() }
      }
    }
  }


  implicit class KafkaProducerOps[F[_]](val self: KafkaProducer[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): KafkaProducer[G] = new KafkaProducer[G] {

      def send[K: skafka.ToBytes, V: skafka.ToBytes](record: ProducerRecord[K, V]) = f(self.send(record))

      def flush = f(self.flush)
    }


    def withMetrics(metrics: Metrics[F])(implicit sync: Sync[F], clock: Clock[F]): KafkaProducer[F] = {
      new KafkaProducer[F] {

        def send[K: skafka.ToBytes, V: skafka.ToBytes](record: ProducerRecord[K, V]) = {
          for {
            ab     <- Latency { self.send(record).attempt }
            (r, l)  = ab
            _      <- r match {
              case Right(r) => metrics.send(record.topic, latency = l, bytes = r.valueSerializedSize getOrElse 0)
              case Left(_)  => metrics.failure(record.topic, l)
            }
            r      <- r.fold(_.raiseError[F, RecordMetadata], _.pure[F])
          } yield r
        }

        def flush = {
          for {
            ab <- Latency { self.flush }
            (r, l) = ab
            _ <- metrics.flush(l)
          } yield r
        }
      }
    }
  }


  trait Metrics[F[_]] {
    
    def send(topic: Topic, latency: Long, bytes: Int): F[Unit]

    def failure(topic: Topic, latency: Long): F[Unit]

    def flush(latency: Long): F[Unit]

    def close(latency: Long): F[Unit]
  }

  object Metrics {

    def apply[F[_] : Sync](metrics: Producer.Metrics): Metrics[F] = new Metrics[F] {

      def send(topic: Topic, latency: Long, bytes: Int) = {
        Sync[F].delay { metrics.send(topic, latency, bytes) }
      }

      def failure(topic: Topic, latency: Long) = {
        Sync[F].delay { metrics.failure(topic, latency) }
      }

      def flush(latency: Long) = {
        Sync[F].delay { metrics.flush(latency) }
      }

      def close(latency: Long) = {
        Sync[F].delay { metrics.close(latency) }
      }
    }


    def const[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def send(topic: Topic, latency: Long, bytes: Int) = unit

      def failure(topic: Topic, latency: Long) = unit

      def flush(latency: Long) = unit

      def close(latency: Long) = unit
    }


    def empty[F[_] : Applicative]: Metrics[F] = const(().pure[F])
  }
}
