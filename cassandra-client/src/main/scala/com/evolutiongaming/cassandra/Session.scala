package com.evolutiongaming.cassandra


import com.datastax.driver.core.{Session => SessionJ, _}
import com.evolutiongaming.cassandra.CassandraHelper._
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.concurrent.FutureHelper._

import scala.collection.JavaConverters._
import scala.concurrent.Future

/**
  * See [[com.datastax.driver.core.Session]]
  */
trait Session {

  def loggedKeyspace: Option[String]

  def init: Future[Session]

  def execute(query: String): Future[ResultSet]

  def execute(query: String, values: Any*): Future[ResultSet]

  def execute(query: String, values: Map[String, AnyRef]): Future[ResultSet]

  def execute(statement: Statement): Future[ResultSet]

  def prepare(query: String): Future[PreparedStatement]

  def prepare(statement: RegularStatement): Future[PreparedStatement]

  def close(): Future[Unit]

  def closed: Boolean

  def state: Session.State
}

object Session {

  def apply(session: SessionJ): Session = new Session {

    def loggedKeyspace = Option(session.getLoggedKeyspace)

    def init = session.initAsync().asScala().map(_ => this)(CurrentThreadExecutionContext)

    def execute(query: String) = session.executeAsync(query).asScala()

    def execute(query: String, values: Any*) = session.executeAsync(query, values).asScala()

    def execute(query: String, values: Map[String, AnyRef]) = session.executeAsync(query, values.asJava).asScala()

    def execute(statement: Statement) = session.executeAsync(statement).asScala()

    def prepare(query: String) = session.prepareAsync(query).asScala()

    def prepare(statement: RegularStatement) = session.prepareAsync(statement).asScala()

    def close() = session.closeAsync().asScala().unit

    def closed = session.isClosed

    def state = State(session.getState)
  }


  /**
    * See [[com.evolutiongaming.cassandra.Session.State]]
    */
  trait State {
    def connectedHosts: Iterable[Host]
    def openConnections(host: Host): Int
    def trashedConnections(host: Host): Int
    def inFlightQueries(host: Host): Int
  }

  object State {
    def apply(state: SessionJ.State): State = new State {
      def connectedHosts = state.getConnectedHosts.asScala
      def openConnections(host: Host) = state.getOpenConnections(host)
      def trashedConnections(host: Host) = state.getTrashedConnections(host)
      def inFlightQueries(host: Host) = state.getInFlightQueries(host)
    }
  }
}
