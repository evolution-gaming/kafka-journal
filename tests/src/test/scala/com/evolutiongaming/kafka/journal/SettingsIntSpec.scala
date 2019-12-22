package com.evolutiongaming.kafka.journal

import akka.actor.ActorSystem
import cats.Parallel
import cats.effect._
import cats.implicits._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{FromFuture, LogOf, ToFuture}
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.kafka.journal.eventual.cassandra._
import com.evolutiongaming.kafka.journal.util.ActorSystemOf
import com.evolutiongaming.kafka.journal.util.PureConfigHelper._
import com.evolutiongaming.kafka.journal.CassandraSuite._
import com.evolutiongaming.scassandra.CassandraClusterOf
import com.evolutiongaming.scassandra.util.FromGFuture
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource


class SettingsIntSpec extends AsyncWordSpec with BeforeAndAfterAll with Matchers {

  override protected def beforeAll(): Unit = {
    IntegrationSuite.start()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }


  private def resources[F[_] : Concurrent : LogOf : Parallel : FromFuture : Timer : ToFuture : ContextShift : FromGFuture](
    origin: Option[Origin],
    cassandraClusterOf: CassandraClusterOf[F]
  ) = {

    def settings(
      config: SchemaConfig)(implicit
      cassandraCluster: CassandraCluster[F],
      cassandraSession: CassandraSession[F]) = {

      for {
        schema   <- SetupSchema[F](config, origin)
        settings <- SettingsCassandra.of[F](schema, origin)
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
      ConfigSource
        .fromConfig(system.settings.config)
        .at("evolutiongaming.kafka-journal.replicator.cassandra")
        .load[EventualCassandraConfig]
        .liftTo[F]
    }

    for {
      system           <- system
      config           <- Resource.liftF(config(system))
      cassandraCluster <- CassandraCluster.of[F](config.client, cassandraClusterOf, config.retries)
      cassandraSession <- cassandraCluster.session
      settings         <- Resource.liftF(settings(config.schema)(cassandraCluster, cassandraSession))
    } yield settings
  }


  def test[F[_] : Concurrent : Parallel : FromFuture : ToFuture : Timer : ContextShift : FromGFuture](
    cassandraClusterOf: CassandraClusterOf[F]
  ): F[Unit] = {

    implicit val logOf = LogOf.empty[F]

    for {
      origin    <- Origin.hostName[F]
      timestamp <- Clock[F].instant
      result    <- resources[F](origin, cassandraClusterOf).use { settings =>

        val setting = Setting(key = "key", value = "value", timestamp = timestamp, origin = origin)

        def fix(setting: Setting) = {
          setting.copy(
            timestamp = timestamp)
        }
        val all = for {
          settings <- settings.all.toList
        } yield for {
          setting  <- settings
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
          _ <- Sync[F].delay { a shouldEqual None }
          a <- all
          _ <- Sync[F].delay { a shouldEqual Nil }
          a <- remove(setting.key)
          _ <- Sync[F].delay { a shouldEqual None }

          a <- settings.set(setting.key, setting.value)
          _ <- Sync[F].delay { a shouldEqual None }
          a <- get(setting.key)
          _ <- Sync[F].delay { a shouldEqual Some(setting) }
          a <- setIfEmpty(setting.key, setting.value)
          _ <- Sync[F].delay { a shouldEqual Some(setting) }
          a <- get(setting.key)
          _ <- Sync[F].delay { a shouldEqual Some(setting) }
          a <- all
          _ <- Sync[F].delay { a shouldEqual List(setting) }

          a <- remove(setting.key)
          _ <- Sync[F].delay { a shouldEqual Some(setting) }
          a <- get(setting.key)
          _ <- Sync[F].delay { a shouldEqual None }
          a <- all
          _ <- Sync[F].delay { a shouldEqual Nil }
          a <- remove(setting.key)
          _ <- Sync[F].delay { a shouldEqual None }
          a <- setIfEmpty(setting.key, setting.value)
          _ <- Sync[F].delay { a shouldEqual None }
        } yield {}
      }
    } yield {
      result
    }
  }


  "Settings" should {

    "set, get, all, remove" in {
      test[IO](cassandraClusterOf).run()
    }
  }
}
