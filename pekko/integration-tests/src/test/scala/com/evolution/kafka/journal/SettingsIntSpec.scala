package com.evolution.kafka.journal

import cats.effect.*
import cats.effect.implicits.*
import cats.implicits.*
import com.evolution.kafka.journal.IOSuite.*
import com.evolution.kafka.journal.cassandra.{CassandraConsistencyConfig, SettingsCassandra as SettingsCassandra2}
import com.evolution.kafka.journal.eventual.cassandra.*
import com.evolution.kafka.journal.util.PureConfigHelper.*
import com.evolutiongaming.catshelper.LogOf
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import pureconfig.ConfigSource

class SettingsIntSpec extends AsyncWordSpec with BeforeAndAfterAll with Matchers {

  import IntegrationTestInstances.*

  override protected def beforeAll(): Unit = {
    IntegrationSuite.start()
    super.beforeAll()
  }

  private def resources(origin: Option[Origin]): ResourceIO[Settings[IO]] = {
    implicit val logOf: LogOf[IO] = LogOf.empty

    def settings(
      config: SchemaConfig,
    )(implicit
      cassandraCluster: CassandraCluster[IO],
      cassandraSession: CassandraSession[IO],
    ) = {

      for {
        schema <- SetupSchema[IO](config, origin, CassandraConsistencyConfig.default)
        settings <- SettingsCassandra2.of[IO](schema.setting, origin, CassandraConsistencyConfig.default)
      } yield settings
    }

    def loadConfigIo: IO[EventualCassandraConfig] = {
      for {
        rawConfig <- IO.blocking { ConfigFactory.load("replicator.conf") }
        config <- ConfigSource
          .fromConfig(rawConfig)
          .at("evolutiongaming.kafka-journal.replicator.cassandra")
          .load[EventualCassandraConfig]
          .liftTo[IO]
      } yield config
    }

    for {
      config <- loadConfigIo.toResource
      cassandraCluster <- CassandraCluster.make[IO](config.client, cassandraClusterOf, config.retries)
      cassandraSession <- cassandraCluster.session
      settings <- settings(config.schema)(cassandraCluster, cassandraSession).toResource
    } yield settings
  }

  private def runTestIo: IO[Unit] = {
    for {
      origin <- Origin.hostName[IO]
      timestamp <- IO.realTimeInstant
      result <- resources(origin).use { settings =>
        val setting = Setting(key = "key", value = "value", timestamp = timestamp, origin = origin)

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
          _ <- IO { a shouldEqual None }
          a <- all
          _ <- IO { a shouldEqual Nil }
          a <- remove(setting.key)
          _ <- IO { a shouldEqual None }

          a <- settings.set(setting.key, setting.value)
          _ <- IO { a shouldEqual None }
          a <- get(setting.key)
          _ <- IO { a shouldEqual setting.some }
          a <- all
          _ <- IO { a shouldEqual List(setting) }

          a <- remove(setting.key)
          _ <- IO { a shouldEqual setting.some }
          a <- get(setting.key)
          _ <- IO { a shouldEqual None }
          a <- all
          _ <- IO { a shouldEqual Nil }
          a <- remove(setting.key)
          _ <- IO { a shouldEqual None }

          // clean up the database
          _ <- remove(setting.key)
        } yield {}
      }
    } yield {
      result
    }
  }

  "CassandraSettings" should {
    "set, get, all, remove" in {
      runTestIo.run()
    }
  }

}
