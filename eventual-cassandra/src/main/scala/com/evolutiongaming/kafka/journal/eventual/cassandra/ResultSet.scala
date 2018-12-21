package com.evolutiongaming.kafka.journal.eventual.cassandra


import com.datastax.driver.core.Row
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.scassandra.syntax._

import scala.concurrent.Future

trait ResultSet {

  def one(): Row

  def isFullyFetched: Boolean

  def getAvailableWithoutFetching: Int

  // TODO
  def fetchMoreResults(): Future[ResultSet]

  def all(): java.util.List[Row]
}

object ResultSet {

  def apply(resultSet: com.datastax.driver.core.ResultSet): ResultSet = {

    new ResultSet {

      def one() = resultSet.one()

      def isFullyFetched = resultSet.isFullyFetched

      def getAvailableWithoutFetching = resultSet.getAvailableWithoutFetching

      def fetchMoreResults() = {
        implicit val ec = CurrentThreadExecutionContext
        for {
          resultSet <- resultSet.fetchMoreResults().asScala()
        } yield {
          apply(resultSet)
        }
      }

      def all() = resultSet.all()
    }
  }
}