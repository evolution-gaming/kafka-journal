package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import cats.Parallel
import cats.effect._
import cats.effect.syntax.resource._
import cats.syntax.all._
import com.evolutiongaming.catshelper.{FromFuture, LogOf}
import com.evolutiongaming.kafka.journal.CassandraSuite._
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.cassandra.{CassandraConsistencyConfig, SettingsCassandra => SettingsCassandra2}
import com.evolutiongaming.kafka.journal.eventual.cassandra._
import com.evolutiongaming.kafka.journal.eventual.cassandra.{SettingsCassandra => SettingsCassandra1}
import com.evolutiongaming.kafka.journal.util.ActorSystemOf
import com.evolutiongaming.kafka.journal.util.PureConfigHelper._
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

  override protected def afterAll(): Unit =
    super.afterAll()

  private def resources[F[_]: Async: LogOf: Parallel: FromFuture](
    origin: Option[Origin],
    cassandraClusterOf: CassandraClusterOf[F],
    legacySettings: Boolean,
  ) = {

    def settings1(
      config: SchemaConfig,
    )(implicit cassandraCluster: CassandraCluster[F], cassandraSession: CassandraSession[F]) =
      for {
        schema   <- SetupSchema[F](config, origin, EventualCassandraConfig.ConsistencyConfig.default)
        settings <- SettingsCassandra1.of[F](schema, origin, EventualCassandraConfig.ConsistencyConfig.default)
      } yield settings

    def settings2(
      config: SchemaConfig,
    )(implicit cassandraCluster: CassandraCluster[F], cassandraSession: CassandraSession[F]) =
      for {
        schema   <- SetupSchema[F](config, origin, EventualCassandraConfig.ConsistencyConfig.default)
        settings <- SettingsCassandra2.of[F](schema.setting, origin, CassandraConsistencyConfig.default)
      } yield settings

    val system = {
      val config = Sync[F].delay(ConfigFactory.load("replicator.conf"))
      for {
        config <- config.toResource
        system <- ActorSystemOf[F](getClass.getSimpleName, config.some)
      } yield system
    }

    def config(system: ActorSystem) =
      ConfigSource
        .fromConfig(system.settings.config)
        .at("evolutiongaming.kafka-journal.replicator.cassandra")
        .load[EventualCassandraConfig]
        .liftTo[F]

    for {
      system           <- system
      config           <- config(system).toResource
      cassandraCluster <- CassandraCluster.of[F](config.client, cassandraClusterOf, config.retries)
      cassandraSession <- cassandraCluster.session
      settings <-
        if (legacySettings) {
          settings1(config.schema)(cassandraCluster, cassandraSession).toResource
        } else {
          settings2(config.schema)(cassandraCluster, cassandraSession).toResource
        }
    } yield settings
  }

  def test[F[_]: Async: Parallel: FromFuture](
    cassandraClusterOf: CassandraClusterOf[F],
    legacySettings: Boolean,
  ): F[Unit] = {

    implicit val logOf = LogOf.empty[F]

    for {
      origin    <- Origin.hostName[F]
      timestamp <- Clock[F].realTimeInstant
      result <- resources[F](origin, cassandraClusterOf, legacySettings).use { settings =>
        val setting = Setting(key = "key", value = "value", timestamp = timestamp, origin = origin)

        def fix(setting: Setting) =
          setting.copy(timestamp = timestamp)
        val all = for {
          settings <- settings.all.toList
        } yield for {
          setting <- settings
          if setting.key =!= "schema-version"
        } yield fix(setting)

        def get(key: Setting.Key) = for {
          setting <- settings.get(key)
        } yield for {
          setting <- setting
        } yield fix(setting)

        def setIfEmpty(key: Setting.Key, value: Setting.Value) =
          for {
            setting <- settings.setIfEmpty(key, value)
          } yield for {
            setting <- setting
          } yield fix(setting)

        def remove(key: Setting.Key) =
          for {
            setting <- settings.remove(key)
          } yield for {
            setting <- setting
          } yield fix(setting)

        for {
          a <- get(setting.key)
          _ <- Sync[F].delay(a shouldEqual None)
          a <- all
          _ <- Sync[F].delay(a shouldEqual Nil)
          a <- remove(setting.key)
          _ <- Sync[F].delay(a shouldEqual None)

          a <- settings.set(setting.key, setting.value)
          _ <- Sync[F].delay(a shouldEqual None)
          a <- get(setting.key)
          _ <- Sync[F].delay(a shouldEqual setting.some)
          a <- setIfEmpty(setting.key, setting.value)
          _ <- Sync[F].delay(a shouldEqual setting.some)
          a <- get(setting.key)
          _ <- Sync[F].delay(a shouldEqual setting.some)
          a <- all
          _ <- Sync[F].delay(a shouldEqual List(setting))

          a <- remove(setting.key)
          _ <- Sync[F].delay(a shouldEqual setting.some)
          a <- get(setting.key)
          _ <- Sync[F].delay(a shouldEqual None)
          a <- all
          _ <- Sync[F].delay(a shouldEqual Nil)
          a <- remove(setting.key)
          _ <- Sync[F].delay(a shouldEqual None)
          a <- setIfEmpty(setting.key, setting.value)
          _ <- Sync[F].delay(a shouldEqual None)

          // clean up the database
          _ <- remove(setting.key)
        } yield {}
      }
    } yield result
  }

  "CassandraSettings" should {
    "set, get, all, remove" in {
      // run two tests in sequence, to avoid concurrency issues
      val program =
        test[IO](cassandraClusterOf, legacySettings = false) *>
          test[IO](cassandraClusterOf, legacySettings = true)
      program.run()
    }
  }

}
