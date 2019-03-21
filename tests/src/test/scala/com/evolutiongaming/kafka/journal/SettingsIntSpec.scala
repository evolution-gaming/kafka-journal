package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.time.temporal.ChronoUnit

import akka.actor.ActorSystem
import cats.effect._
import cats.implicits._
import cats.temp.par.Par
import com.evolutiongaming.catshelper.{FromFuture, ToFuture}
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.eventual.cassandra._
import com.evolutiongaming.kafka.journal.util.{ActorSystemOf, FromGFuture}
import com.typesafe.config.ConfigFactory
import org.scalatest.{AsyncWordSpec, BeforeAndAfterAll, Matchers}

class SettingsIntSpec extends AsyncWordSpec with BeforeAndAfterAll with Matchers {

  override protected def beforeAll(): Unit = {
    IntegrationSuite.start()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }


  private def resources[F[_] : Concurrent : LogOf : Par : FromFuture : Clock : ToFuture : ContextShift : FromGFuture] = {

    def settings(
      config: SchemaConfig)(implicit
      cassandraCluster: CassandraCluster[F],
      cassandraSession: CassandraSession[F]) = {

      for {
        schema   <- SetupSchema[F](config)
        settings <- SettingsCassandra.of[F](schema)
      } yield settings
    }

    val system = {
      val config = Sync[F].delay { ConfigFactory.load("replicator.conf") }
      for {
        config <- Resource.liftF(config)
        system <- ActorSystemOf[F](getClass.getSimpleName, Some(config))
      } yield system
    }

    def config(system: ActorSystem) = {
      for {
        conf   <- Sync[F].delay { system.settings.config.getConfig("evolutiongaming.kafka-journal.replicator") }
        config <- Sync[F].delay { EventualCassandraConfig(conf.getConfig("cassandra")) }
      } yield config
    }

    for {
      system           <- system
      config           <- Resource.liftF(config(system))
      cassandraCluster <- CassandraCluster.of[F](config.client, config.retries, executor)
      cassandraSession <- cassandraCluster.session
      settings         <- Resource.liftF(settings(config.schema)(cassandraCluster, cassandraSession))
    } yield settings
  }


  def test[F[_] : Concurrent : Par : FromFuture : ToFuture : Clock : ContextShift]: F[Unit] = {

    implicit val logOf = LogOf.empty[F]

    val timestamp = Instant.now().truncatedTo(ChronoUnit.MILLIS)

    val setting = Setting(key = "key", value = "value", timestamp = timestamp, origin = Some("origin"))

    def fix(setting: Setting) = {
      setting.copy(
        timestamp = timestamp,
        origin = Some("origin"))
    }

    resources[F].use { settings =>

      val all = for {
        settings <- settings.all.toList
      } yield for {
        setting  <- settings
        if setting.key != "schema-version"
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

      def setIfEmpty(key: Setting.Key, value: Setting.Value) = {
        for {
          setting <- settings.setIfEmpty(key, value)
        } yield for {
          setting <- setting
        } yield {
          fix(setting)
        }
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
        _ = a shouldEqual None
        a <- all
        _ = a shouldEqual Nil
        a <- remove(setting.key)
        _ = a shouldEqual None

        a <- settings.set(setting.key, setting.value)
        _ = a shouldEqual None
        a <- get(setting.key)
        _ = a shouldEqual Some(setting)
        a <- setIfEmpty(setting.key, setting.value)
        _ = a shouldEqual Some(setting)
        a <- get(setting.key)
        _ = a shouldEqual Some(setting)
        a <- all
        _ = a shouldEqual List(setting)

        a <- remove(setting.key)
        _ = a shouldEqual Some(setting)
        a <- get(setting.key)
        _ = a shouldEqual None
        a <- all
        _ = a shouldEqual Nil
        a <- remove(setting.key)
        _ = a shouldEqual None
        a <- setIfEmpty(setting.key, setting.value)
        _ = a shouldEqual None
      } yield {}
    }
  }


  "Settings" should {

    "set, get, all, remove" in {
      test[IO].run()
    }
  }
}
