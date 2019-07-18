package com.evolutiongaming.kafka.journal

final case class JournalError(
  msg: String,
  cause: Option[Throwable] = None
) extends RuntimeException(msg, cause.orNull)