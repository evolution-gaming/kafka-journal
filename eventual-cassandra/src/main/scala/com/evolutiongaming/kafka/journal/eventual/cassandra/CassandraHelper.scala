package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.{ResultSet, Row, Statement}
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.IO2.ops._
import com.evolutiongaming.kafka.journal.{FromFuture, IO2}
import com.evolutiongaming.scassandra.syntax._

import scala.annotation.tailrec

object CassandraHelper {

  implicit class ResultSetOps(val self: ResultSet) extends AnyVal {

    def foldWhile[F[_] : IO2 : FromFuture, S](fetchThreshold: Int, s: S)(f: Fold[S, Row]): F[Switch[S]] = {

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
        if (ss.stop || fetched) Switch.stop(ss).pure
        else for {
          _ <- IO2[F].from {
            self.fetchMoreResults().asScala()
          }
        } yield Switch.continue(ss)
      }

      fetch.foldWhile(s.continue)
    }
  }


  // TODO move to scassandra
  implicit class StatementOps(val self: Statement) extends AnyVal {

    def trace(enable: Boolean): Statement = {
      if (enable) self.enableTracing()
      else self.disableTracing()
    }
  }
}