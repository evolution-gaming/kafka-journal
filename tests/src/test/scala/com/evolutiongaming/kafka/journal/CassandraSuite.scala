package com.evolutiongaming.kafka.journal

import cats.effect.IO
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.scassandra.CassandraClusterOf

object CassandraSuite {
  implicit lazy val cassandraClusterOf: CassandraClusterOf[IO] = CassandraClusterOf.of[IO].toTry.get
}
