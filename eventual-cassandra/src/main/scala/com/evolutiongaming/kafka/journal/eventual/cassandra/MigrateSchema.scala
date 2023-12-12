package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.MonadThrow
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.Settings
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._

import scala.util.Try

/** Migrates the existing schema to the latest version */
trait MigrateSchema[F[_]] {

  /** Run all built-in migrations
    *
    * @param fresh
    *   Indicates if the schema was just created from scratch, or some tables were already present. The parameter is
    *   taken into consideration if there is no schema version information available in the settings. In this case, if
    *   `true`, then it will be assumed that no migrations are required, and the latest version will be saved into
    *   settings. If `false` then all migration steps will be attempted (because the schema, likely, was created before
    *   migration steps were added).
    */
  def run(fresh: CreateSchema.Fresh)(implicit session: CassandraSession[F]): F[Unit]

}
object MigrateSchema {

  /** Save version of a schema to the settings storage under specific key.
    *
    * @param cassandraSync
    *   Locking mechanism to ensure two migrations are not happening in paralell.
    * @param settings
    *   Storage to get / save the schema version from / to.
    * @param settingKey
    *   A key to use in a setting store. It is important to use a different key for different schemas, to ensure there
    *   is no accidential overwrite if both schemas are located in one keyspace.
    * @param migrations
    *   List of CQL statements to execute. The schema version is equal to the size of this list.
    * @return
    *   The instance of schema migrator.
    */
  def forSettingKey[F[_]: MonadThrow](
    cassandraSync: CassandraSync[F],
    settings: Settings[F],
    settingKey: String,
    migrations: Nel[String]
  ): MigrateSchema[F] = new MigrateSchema[F] {

    def setVersion(version: Int) =
      settings
        .set(settingKey, version.toString)
        .void

    def run(fresh: CreateSchema.Fresh)(implicit session: CassandraSession[F]): F[Unit] = {

      def migrate = {

        def migrate(version: Int) = {
          migrations.toList
            .drop(version + 1)
            .toNel
            .map { migrations =>
              migrations
                .foldLeftM(version) { (version, migration) =>
                  val version1 = version + 1
                  for {
                    _ <- migration.execute.first.void.voidError
                    _ <- setVersion(version1)
                  } yield version1
                }
                .void
            }
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

}
