package com.evolutiongaming.kafka.journal


final case class HostName(value: String) {
  override def toString: Id = value
}

object HostName {

  private val value = com.evolutiongaming.hostname.HostName().map(HostName(_))

  def apply(): Option[HostName] = value
}