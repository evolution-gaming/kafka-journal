package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.kernel.Temporal
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.{Origin, Settings}
import com.evolutiongaming.scassandra.ToCql.implicits._

import scala.util.Try

class SetupSchema[F[_]: MonadThrow](
  cassandraSync: CassandraSync[F],
  settings: Settings[F],
  settingKey: String,
  migrations: List[F[Unit]]
) {

  def migrate(fresh: CreateSchema.Fresh): F[Unit] = {

    def setVersion(version: Int) =
      settings
        .set(settingKey, version.toString)
        .void

    def migrate = {

      def migrate(version: Int) =
        migrations.toList
          .drop(version + 1)
          .toNel
          .map { migrations =>
            migrations
              .foldLeftM(version) { (version, migration) =>
                val version1 = version + 1
                for {
                  _ <- migration
                  _ <- setVersion(version1)
                } yield version1
              }
              .void
          }

      settings
        .get(settingKey)
        .map { setting =>
          setting
            .flatMap { a =>
              Try.apply { a.value.toInt }.toOption
            }
            .fold {
              if (fresh) {
                val version = migrations.size - 1
                if (version >= 0) {
                  setVersion(version).some
                } else {
                  none[F[Unit]]
                }
              } else {
                migrate(-1)
              }
            } { version =>
              migrate(version)
            }
        }
    }

    migrate.flatMap { migrate1 =>
      migrate1.foldMapM { _ =>
        cassandraSync {
          migrate.flatMap { migrate =>
            migrate.foldMapM(identity)
          }
        }
      }
    }
  }

}

object SetupSchema {

  def apply[F[_]: MonadThrow](
    cassandraSync: CassandraSync[F],
    settings: Settings[F],
    settingKey: String,
    migrations: List[F[Unit]]
  ): SetupSchema[F] =
    new SetupSchema(cassandraSync, settings, settingKey, migrations)

}
