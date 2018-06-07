package com.evolutiongaming.cassandra

import java.io.Closeable

import com.datastax.driver.core.Session


trait CassandraSession extends Closeable {

}

object CassandraSession {

  def apply(session: Session): CassandraSession = {
    ???
  }
}
