package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.util.ToFuture
import com.evolutiongaming.skafka.{Offset, Partition}

import scala.concurrent.ExecutionContext

object HeadCacheAsync {

  def apply[F[_] : ToFuture](headCache: HeadCache[F])(implicit ec: ExecutionContext): HeadCache[Async] = {

    new HeadCache[Async] {

      def apply(key: Key, partition: Partition, marker: Offset) = {
        Async {
          ToFuture[F].apply {
            headCache(key, partition, marker)
          }
        }
      }

      def close = {
        Async {
          ToFuture[F].apply {
            headCache.close
          }
        }
      }
    }
  }
}
