package com.evolution.kafka.journal.cassandra

import cats.MonadThrow
import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import com.evolution.kafka.journal.Settings
import com.evolution.kafka.journal.cassandra.CassandraSync
import com.evolution.kafka.journal.eventual.cassandra.CassandraHelper.*
import com.evolution.kafka.journal.eventual.cassandra.CassandraSession

import scala.util.Try

/**
 * Migrates the existing schema to the latest version
 */
private[journal] trait MigrateSchema[F[_]] {

  /**
   * Run all built-in migrations
   *
   * @param fresh
   *   Indicates if the schema was just auto-created from scratch (`true`), or some tables were
   *   already present (`false`). The parameter is taken into consideration if there is no schema
   *   version information available in the settings. In this case:
   *   - if `true`, then it will be assumed that no migrations are required, and the latest version
   *     will be saved into `settings` table.
   *   - if `false`, then all migration steps will be attempted (because the schema, likely, was
   *     created before migration steps were added).
   */
  def run(
    fresh: MigrateSchema.Fresh,
  )(implicit
    session: CassandraSession[F],
  ): F[Unit]

}

private[journal] object MigrateSchema {

  type Fresh = Boolean

  /**
   * Save version of a schema to the settings storage under specific key.
   *
   * @param cassandraSync
   *   Locking mechanism to ensure two migrations are not happening in paralell.
   * @param settings
   *   Storage to get / save the schema version from / to.
   * @param settingKey
   *   A key to use in a setting store. It is important to use a different key for different
   *   schemas, to ensure there is no accidental overwrite if both schemas are located in one
   *   keyspace.
   * @param migrations
   *   List of CQL statements to execute. The schema version is equal to the size of this list.
   * @return
   *   The instance of schema migrator.
   */
  def forSettingKey[F[_]: MonadThrow](
    cassandraSync: CassandraSync[F],
    settings: Settings[F],
    settingKey: String,
    migrations: Nel[String],
  ): MigrateSchema[F] = new MigrateSchema[F] {

    def setVersion(version: Int): F[Unit] =
      settings
        .set(settingKey, version.toString)
        .void

    def run(
      fresh: MigrateSchema.Fresh,
    )(implicit
      session: CassandraSession[F],
    ): F[Unit] = {

      def migrate: F[Option[F[Unit]]] = {

        def migrate(version: Int): Option[F[Unit]] = {
          migrations
            .toList
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
