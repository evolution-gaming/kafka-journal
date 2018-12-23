package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.util.concurrent.Executors

import cats.effect.{IO, Resource}
import com.evolutiongaming.cassandra
import com.evolutiongaming.cassandra.sync.AutoCreate
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.Origin
import com.evolutiongaming.kafka.journal.util.IOFromFuture
import com.evolutiongaming.scassandra.Session

import scala.concurrent.ExecutionContext

trait CassandraSync[F[_]] {
  def apply[A](f: /*TODO*/ => F[A]): F[A]
}

object CassandraSync {

  def apply[F[_]](implicit F: CassandraSync[F]): CassandraSync[F] = F

  def async(
    schemaConfig: SchemaConfig,
    origin: Option[Origin])(implicit ec: ExecutionContext, session: Session): CassandraSync[Async] = {

    val keyspace = schemaConfig.keyspace
    val autoCreate = if (keyspace.autoCreate) AutoCreate.Table else AutoCreate.None
    async(
      keyspace = keyspace.name,
      table = schemaConfig.locksTable,
      autoCreate = autoCreate,
      origin = origin)
  }

  def async(
    keyspace: String,
    table: String,
    autoCreate: AutoCreate,
    origin: Option[Origin])(implicit ec: ExecutionContext, session: Session): CassandraSync[Async] = {

    new CassandraSync[Async] {

      def apply[A](f: => Async[A]) = {
        implicit val es = Executors.newScheduledThreadPool(2)
        val cassandraSync = cassandra.sync.CassandraSync(
          keyspace = keyspace,
          table = table,
          autoCreate = autoCreate)
        val future = cassandraSync(id = "kafka-journal", metadata = origin.map(_.value))(f.future)
        future.onComplete { _ => es.shutdown() }
        Async(future)
      }
    }
  }

  def io(config: SchemaConfig, origin: Option[Origin])(implicit session: Session): CassandraSync[IO] = {

    val keyspace = config.keyspace
    val autoCreate = if (keyspace.autoCreate) AutoCreate.Table else AutoCreate.None
    io(
      keyspace = keyspace.name,
      table = config.locksTable,
      autoCreate = autoCreate,
      origin = origin)
  }

  def io(
    keyspace: String,
    table: String,
    autoCreate: AutoCreate,
    origin: Option[Origin])(implicit session: Session): CassandraSync[IO] = {

    new CassandraSync[IO] {

      def apply[A](f: => IO[A]) = {

        val es = {
          val es = IO.delay { Executors.newScheduledThreadPool(2) }
          Resource.make(es) { es => IO.delay { es.shutdown() } }
        }

        es.use { implicit es =>
          val cassandraSync = cassandra.sync.CassandraSync(
            keyspace = keyspace,
            table = table,
            autoCreate = autoCreate)
          IOFromFuture {
            cassandraSync(id = "kafka-journal", metadata = origin.map(_.value)) {
              f.unsafeToFuture()
            }
          }
        }
      }
    }
  }
}