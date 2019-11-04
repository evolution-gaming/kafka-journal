package com.evolutiongaming.kafka.journal

import scala.concurrent.duration.FiniteDuration

sealed abstract class HeadCacheError

object HeadCacheError {

  def invalid: HeadCacheError = Invalid

  def timeout(timeout: FiniteDuration): HeadCacheError = Timeout(timeout)


  case object Invalid extends HeadCacheError

  final case class Timeout(timeout: FiniteDuration) extends HeadCacheError
}
