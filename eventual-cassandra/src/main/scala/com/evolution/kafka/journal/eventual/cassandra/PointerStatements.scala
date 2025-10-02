package com.evolution.kafka.journal.eventual.cassandra

import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.scassandra.syntax.*

private[journal] object PointerStatements {

  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.toCql } (
       |topic text,
       |partition int,
       |offset bigint,
       |created timestamp,
       |updated timestamp,
       |PRIMARY KEY ((topic), partition))
       |""".stripMargin
  }
}
