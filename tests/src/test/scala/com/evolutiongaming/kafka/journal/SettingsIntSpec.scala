package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import cats.Parallel
import cats.effect.*
import cats.effect.syntax.resource.*
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromFuture, LogOf}
import com.evolutiongaming.kafka.journal.CassandraSuite.*
import com.evolutiongaming.kafka.journal.IOSuite.*
import com.evolutiongaming.kafka.journal.cassandra.{CassandraConsistencyConfig, SettingsCassandra as SettingsCassandra2}
import com.evolutiongaming.kafka.journal.eventual.cassandra.*
import com.evolutiongaming.kafka.journal.util.ActorSystemOf
import com.evolutiongaming.kafka.journal.util.PureConfigHelper.*
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import pureconfig.ConfigSource

class SettingsIntSpec extends AsyncWordSpec with BeforeAndAfterAll with Matchers {

  override protected def beforeAll(): Unit = {
    IntegrationSuite.start()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  private def resources[F[_]: Async: LogOf: Parallel: FromFuture](
    origin: Option[Origin],
    cassandraClusterOf: CassandraClusterOf[F],
  ) = {

    def settings(config: SchemaConfig)(implicit cassandraCluster: CassandraCluster[F], cassandraSession: CassandraSession[F]) = {

      for {
        schema   <- SetupSchema[F](config, origin, CassandraConsistencyConfig.default)
        settings <- SettingsCassandra2.of[F](schema.setting, origin, CassandraConsistencyConfig.default)
      } yield settings
    }

    val system = {
      val config = Sync[F].delay { ConfigFactory.load("replicator.conf") }
      for {
        config <- config.toResource
        system <- ActorSystemOf[F](getClass.getSimpleName, config.some)
      } yield system
    }

    def config(system: ActorSystem) = {
      ConfigSource
        .fromConfig(system.settings.config)
        .at("evolutiongaming.kafka-journal.replicator.cassandra")
        .load[EventualCassandraConfig]
        .liftTo[F]
    }

    for {
      system           <- system
      config           <- config(system).toResource
      cassandraCluster <- CassandraCluster.of[F](config.client, cassandraClusterOf, config.retries)
      cassandraSession <- cassandraCluster.session
      settings         <- settings(config.schema)(cassandraCluster, cassandraSession).toResource
    } yield settings
  }

  def test[F[_]: Async: Parallel: FromFuture](cassandraClusterOf: CassandraClusterOf[F]): F[Unit] = {

    implicit val logOf = LogOf.empty[F]

    for {
      origin    <- Origin.hostName[F]
      timestamp <- Clock[F].realTimeInstant
      result <- resources[F](origin, cassandraClusterOf).use { settings =>
        val setting = Setting(key = "key", value = "value", timestamp = timestamp, origin = origin)

        def fix(setting: Setting) = {
          setting.copy(timestamp = timestamp)
        }
        val all = for {
          settings <- settings.all.toList
        } yield for {
          setting <- settings
          if setting.key =!= "schema-version"
        } yield {
          fix(setting)
        }

        def get(key: Setting.Key) = for {
          setting <- settings.get(key)
        } yield for {
          setting <- setting
        } yield {
          fix(setting)
        }

        def remove(key: Setting.Key) = {
          for {
            setting <- settings.remove(key)
          } yield for {
            setting <- setting
          } yield {
            fix(setting)
          }
        }

        for {
          a <- get(setting.key)
          _ <- Sync[F].delay { a shouldEqual None }
          a <- all
          _ <- Sync[F].delay { a shouldEqual Nil }
          a <- remove(setting.key)
          _ <- Sync[F].delay { a shouldEqual None }

          a <- settings.set(setting.key, setting.value)
          _ <- Sync[F].delay { a shouldEqual None }
          a <- get(setting.key)
          _ <- Sync[F].delay { a shouldEqual setting.some }
          a <- all
          _ <- Sync[F].delay { a shouldEqual List(setting) }

          a <- remove(setting.key)
          _ <- Sync[F].delay { a shouldEqual setting.some }
          a <- get(setting.key)
          _ <- Sync[F].delay { a shouldEqual None }
          a <- all
          _ <- Sync[F].delay { a shouldEqual Nil }
          a <- remove(setting.key)
          _ <- Sync[F].delay { a shouldEqual None }

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
      val program = test[IO](cassandraClusterOf)
      program.run()
    }
  }

}
