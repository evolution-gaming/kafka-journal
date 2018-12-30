package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.util.concurrent.Executors

import cats.effect.{Resource, Sync}
import com.evolutiongaming.cassandra
import com.evolutiongaming.cassandra.sync.AutoCreate
import com.evolutiongaming.kafka.journal.Origin
import com.evolutiongaming.kafka.journal.util.{FromFuture, ToFuture}
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.scassandra.Session

trait CassandraSync[F[_]] {
  def apply[A](fa: F[A]): F[A]
}

object CassandraSync {

  def apply[F[_]](implicit F: CassandraSync[F]): CassandraSync[F] = F

  def apply[F[_] : Sync : FromFuture : ToFuture](config: SchemaConfig, origin: Option[Origin])(implicit session: Session): CassandraSync[F] = {

    val keyspace = config.keyspace
    val autoCreate = if (keyspace.autoCreate) AutoCreate.Table else AutoCreate.None
    apply(
      keyspace = keyspace.name,
      table = config.locksTable,
      autoCreate = autoCreate,
      origin = origin)
  }

  def apply[F[_] : Sync : FromFuture : ToFuture](
    keyspace: String,
    table: String,
    autoCreate: AutoCreate,
    origin: Option[Origin])(implicit session: Session): CassandraSync[F] = {

    new CassandraSync[F] {

      def apply[A](fa: F[A]) = {

        val es = Resource.make {
          Sync[F].delay { Executors.newScheduledThreadPool(2) }
        } { es =>
          Sync[F].delay { es.shutdown() }
        }

        es.use { implicit es =>
          val cassandraSync = cassandra.sync.CassandraSync(
            keyspace = keyspace,
            table = table,
            autoCreate = autoCreate)

          FromFuture[F].apply {
            cassandraSync(id = "kafka-journal", metadata = origin.map(_.value)) {
              fa.unsafeToFuture()
            }
          }
        }
      }
    }
  }
}