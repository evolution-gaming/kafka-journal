package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import cats.{Monad, Parallel}
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.skafka.{Partition, Topic}

object Pointer2Statements {

  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.toCql } (
       |topic text,
       |partition int,
       |offset bigint,
       |created timestamp,
       |updated timestamp,
       |PRIMARY KEY ((topic, partition)))
       |""".stripMargin
  }

  object SelectIn {

    def of[F[_]: Monad: Parallel: CassandraSession](
      name: TableName,
      consistencyConfig: ConsistencyConfig.Read
    ): F[PointerStatements.SelectIn[F]] = {

      PointerStatements.Select
        .of[F](name, consistencyConfig)
        .map { select =>
          (topic: Topic, partitions: Nel[Partition]) => {
            partitions
              .toList
              .parFlatTraverse { partition =>
                select
                  .apply(topic, partition)
                  .map { offset =>
                    offset
                      .toList
                      .map { offset => partition -> offset }
                  }
              }
              .map { _.toMap }
          }
        }

    }
  }

  object SelectTopics {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: ConsistencyConfig.Read
    ): F[PointerStatements.SelectTopics[F]] = {

      val query = s"""SELECT DISTINCT topic, partition FROM ${ name.toCql }""".stripMargin

      query
        .prepare
        .map { prepared =>
          () => {
            prepared
              .bind()
              .setConsistencyLevel(consistencyConfig.value)
              .execute
              .toList
              .map { records =>
                records
                  .map { _.decode[Topic]("topic") }
                  .toSortedSet
              }
          }
        }
    }
  }

}
