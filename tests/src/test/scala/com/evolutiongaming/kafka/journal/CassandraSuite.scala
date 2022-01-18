package com.evolutiongaming.kafka.journal

import cats.effect.IO
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.kafka.journal.IOSuite._
import com.evolutiongaming.scassandra.CassandraClusterOf

object CassandraSuite {
  import cats.effect.unsafe.implicits.global

  implicit lazy val cassandraClusterOf: CassandraClusterOf[IO] = CassandraClusterOf.of[IO].toTry.get
}
