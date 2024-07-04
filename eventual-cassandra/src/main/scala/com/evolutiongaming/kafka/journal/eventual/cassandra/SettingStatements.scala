package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.Setting
import com.evolutiongaming.kafka.journal.Setting.Key
import com.evolutiongaming.kafka.journal.cassandra.SettingStatements as SettingStatements2
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow, TableName}
import com.evolutiongaming.sstream.Stream

@deprecated(
  since   = "3.3.9",
  message = "Use a class from `com.evolutiongaming.kafka.journal.cassandra` (without `eventual` part) package instead",
)
object SettingStatements {

  implicit val encodeRowSetting: EncodeRow[Setting] = SettingStatements2.encodeRowSetting

  implicit val decodeRowSetting: DecodeRow[Setting] = SettingStatements2.decodeRowSetting

  def createTable(name: TableName): String = SettingStatements2.createTable(name)

  trait Select[F[_]] { select =>
    def apply(key: Key): F[Option[Setting]]
    private[cassandra] def toSettingStatements2: SettingStatements2.Select[F] =
      key => select(key)
  }

  object Select {

    private[cassandra] def apply[F[_]](select2: SettingStatements2.Select[F]): Select[F] =
      key => select2(key)

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: ConsistencyConfig.Read,
    ): F[Select[F]] =
      SettingStatements2.Select.of(name, consistencyConfig.toCassandraConsistencyConfig).map(Select(_))
  }

  type All[F[_]] = Stream[F, Setting]

  object All {

    def of[F[_]: Monad: CassandraSession](name: TableName, consistencyConfig: ConsistencyConfig.Read): F[All[F]] =
      SettingStatements2.All.of(name, consistencyConfig.toCassandraConsistencyConfig)

  }

  trait Insert[F[_]] { insert =>
    def apply(setting: Setting): F[Unit]
    private[cassandra] def toSettingStatements2: SettingStatements2.Insert[F] =
      setting => insert(setting)
  }

  object Insert {

    private[cassandra] def apply[F[_]](insert2: SettingStatements2.Insert[F]): Insert[F] =
      setting => insert2(setting)

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: ConsistencyConfig.Write,
    ): F[Insert[F]] =
      SettingStatements2.Insert.of(name, consistencyConfig.toCassandraConsistencyConfig).map(Insert(_))

  }

  trait InsertIfEmpty[F[_]] { insertIfEmpty =>
    def apply(setting: Setting): F[Boolean]
    private[cassandra] def toSettingStatements2: SettingStatements2.InsertIfEmpty[F] =
      setting => insertIfEmpty(setting)
  }

  object InsertIfEmpty {

    private[cassandra] def apply[F[_]](insertIfEmpty2: SettingStatements2.InsertIfEmpty[F]): InsertIfEmpty[F] =
      setting => insertIfEmpty2(setting)

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: ConsistencyConfig.Write,
    ): F[InsertIfEmpty[F]] =
      SettingStatements2.InsertIfEmpty.of(name, consistencyConfig.toCassandraConsistencyConfig).map(InsertIfEmpty(_))

  }

  trait Delete[F[_]] { delete =>
    def apply(key: Key): F[Unit]
    private[cassandra] def toSettingStatements2: SettingStatements2.Delete[F] =
      key => delete(key)
  }

  object Delete {

    private[cassandra] def apply[F[_]](delete2: SettingStatements2.Delete[F]): Delete[F] =
      key => delete2(key)

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: ConsistencyConfig.Write,
    ): F[Delete[F]] =
      SettingStatements2.Delete.of(name, consistencyConfig.toCassandraConsistencyConfig).map(Delete(_))

  }
}
