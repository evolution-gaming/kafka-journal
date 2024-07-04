package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.Clock
import cats.syntax.all._
import cats.{Monad, Parallel}
import com.evolutiongaming.kafka.journal.cassandra.{SettingsCassandra => SettingsCassandra2}
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
import com.evolutiongaming.kafka.journal.{Origin, Settings}
import com.evolutiongaming.scassandra.TableName

@deprecated(
  since   = "3.3.9",
  message = "Use a class from `com.evolutiongaming.kafka.journal.cassandra` (without `eventual` part) package instead",
)
object SettingsCassandra {

  def apply[F[_]: Monad: Clock](
    statements: Statements[F],
    origin: Option[Origin],
  ): Settings[F] =
    SettingsCassandra2(statements.toSettingsCassandra2, origin)

  def of[F[_]: Monad: Parallel: Clock: CassandraSession](
    schema: Schema,
    origin: Option[Origin],
    consistencyConfig: EventualCassandraConfig.ConsistencyConfig,
  ): F[Settings[F]] =
    SettingsCassandra2.of(schema.setting, origin, consistencyConfig.toCassandraConsistencyConfig)

  final case class Statements[F[_]](
    select: SettingStatements.Select[F],
    insert: SettingStatements.Insert[F],
    insertIfEmpty: SettingStatements.InsertIfEmpty[F],
    all: SettingStatements.All[F],
    delete: SettingStatements.Delete[F],
  ) {

    private[cassandra] def toSettingsCassandra2: SettingsCassandra2.Statements[F] =
      SettingsCassandra2.Statements(
        select        = select.toSettingStatements2,
        insert        = insert.toSettingStatements2,
        insertIfEmpty = insertIfEmpty.toSettingStatements2,
        all           = all,
        delete        = delete.toSettingStatements2,
      )

  }

  object Statements {

    private[cassandra] def apply[F[_]](statements2: SettingsCassandra2.Statements[F]): Statements[F] =
      Statements(
        select        = SettingStatements.Select(statements2.select),
        insert        = SettingStatements.Insert(statements2.insert),
        insertIfEmpty = SettingStatements.InsertIfEmpty(statements2.insertIfEmpty),
        all           = statements2.all,
        delete        = SettingStatements.Delete(statements2.delete),
      )

    def of[F[_]: Monad: Parallel: CassandraSession](
      table: TableName,
      consistencyConfig: EventualCassandraConfig.ConsistencyConfig,
    ): F[Statements[F]] = {
      val statements2 = SettingsCassandra2.Statements.of(table, consistencyConfig.toCassandraConsistencyConfig)
      statements2.map(Statements(_))
    }
  }
}
