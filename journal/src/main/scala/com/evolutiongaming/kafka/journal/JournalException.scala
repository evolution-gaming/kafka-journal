package com.evolutiongaming.kafka.journal

case class JournalException(
  key: Key,
  message: Option[String] = None,
  cause: Option[Throwable] = None) extends RuntimeException(message.orNull, cause.orNull)

object JournalException {
  def apply(key: Key, message: String): JournalException = JournalException(key, Some(message))
}
