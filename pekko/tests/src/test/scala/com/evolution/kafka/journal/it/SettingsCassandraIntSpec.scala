package com.evolution.kafka.journal.it

import cats.effect.*
import cats.implicits.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.cassandra.{CassandraConsistencyConfig, SettingsCassandra}
import com.evolution.kafka.journal.eventual.cassandra.*
import com.evolution.kafka.journal.{Setting, Settings}
import org.scalatest.wordspec.AsyncWordSpec

class SettingsCassandraIntSpec extends AsyncWordSpec with JournalSuite {
  import ItInstances.*

  private def setupSchemaAndMakeSettings(implicit
    cassandraCluster: CassandraCluster[IO],
    cassandraSession: CassandraSession[IO],
  ): IO[Settings[IO]] = {
    for {
      schema <- SetupSchema[IO](config.eventualCassandra.schema, origin.some, CassandraConsistencyConfig.default)
      settings <- SettingsCassandra.of[IO](schema.setting, origin.some, CassandraConsistencyConfig.default)
    } yield settings
  }

  private def resources: ResourceIO[Settings[IO]] = {
    for {
      cassandraCluster <- CassandraCluster.make(
        config.eventualCassandra.client,
        cassandraClusterOf,
        config.eventualCassandra.retries,
      )
      cassandraSession <- cassandraCluster.session
      settings <- setupSchemaAndMakeSettings(cassandraCluster, cassandraSession).toResource
    } yield settings
  }

  private def testProgramIo: IO[Unit] = {
    for {
      timestamp <- Clock[IO].realTimeInstant
      result <- resources.use { settings =>
        val setting = Setting(key = "key", value = "value", timestamp = timestamp, origin = origin.some)

        def fix(setting: Setting) = {
          setting.copy(timestamp = timestamp)
        }
        val all = for {
          settings <- settings.all.toList
        } yield
          for {
            setting <- settings
            if setting.key =!= "schema-version"
          } yield {
            fix(setting)
          }

        def get(key: Setting.Key) = for {
          setting <- settings.get(key)
        } yield
          for {
            setting <- setting
          } yield {
            fix(setting)
          }

        def remove(key: Setting.Key) = {
          for {
            setting <- settings.remove(key)
          } yield
            for {
              setting <- setting
            } yield {
              fix(setting)
            }
        }

        for {
          a <- get(setting.key)
          _ = a shouldEqual None
          a <- all
          _ = a shouldEqual Nil
          a <- remove(setting.key)
          _ = a shouldEqual None

          a <- settings.set(setting.key, setting.value)
          _ = a shouldEqual None
          a <- get(setting.key)
          _ = a shouldEqual setting.some
          a <- all
          _ = a shouldEqual List(setting)

          a <- remove(setting.key)
          _ = a shouldEqual setting.some
          a <- get(setting.key)
          _ = a shouldEqual None
          a <- all
          _ = a shouldEqual Nil
          a <- remove(setting.key)
          _ = a shouldEqual None

          // clean up the database
          _ <- remove(setting.key)
        } yield {}
      }
    } yield {
      result
    }
  }

  "SettingsCassandra" should {
    "set, get, all, remove" in {
      testProgramIo.run()
    }
  }

}
