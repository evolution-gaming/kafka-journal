package com.evolution.kafka.journal

import cats.effect.IO
import com.evolutiongaming.catshelper.CatsHelper.*
import com.evolutiongaming.scassandra.CassandraClusterOf

object CassandraSuite {
  import cats.effect.unsafe.implicits.global

  implicit lazy val cassandraClusterOf: CassandraClusterOf[IO] = CassandraClusterOf.of[IO].toTry.get
}
