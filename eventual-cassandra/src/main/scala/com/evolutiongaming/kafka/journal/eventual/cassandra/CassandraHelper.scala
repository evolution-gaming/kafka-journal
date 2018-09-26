package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.{ResultSet, Row}
import com.evolutiongaming.cassandra.CassandraHelper._
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext

object CassandraHelper {

  implicit class ResultSetOps(val self: ResultSet) extends AnyVal {

    def foldWhile[S](fetchThreshold: Int, s: S)(f: Fold[S, Row])(implicit ec: ExecutionContext /*TODO remove*/): Async[Switch[S]] = {

      @tailrec def foldWhile(s: S, available: Int): Switch[S] = {
        if (available == 0) s.continue
        else {
          if (available == fetchThreshold) self.fetchMoreResults()
          val row = self.one()
          val switch = f(s, row)
          if (switch.stop) switch
          else foldWhile(switch.s, available - 1)
        }
      }

      val fetch = (s: Switch[S]) => {
        val fetched = self.isFullyFetched
        val available = self.getAvailableWithoutFetching
        val ss = foldWhile(s.s, available)
        if (ss.stop || fetched) Switch.stop(ss).async
        else for {
          _ <- self.fetchMoreResults().asScala().async
        } yield Switch.continue(ss)
      }

      fetch.foldWhile(s.continue)
    }
  }
}
