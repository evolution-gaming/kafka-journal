package com.evolutiongaming.kafka.journal

import com.evolutiongaming.skafka.Header
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import play.api.libs.json.Json

object ActionConverters {

  def toHeader(a: Action): Header = {
    val json = Json.toJson(a)
    val bytes = Json.toBytes(json)
    Header("journal.action", bytes)
  }

  def toAction(record: ConsumerRecord[String, Array[Byte]]): Action = {
    val headers = record.headers
    val header = headers.find { _.key == "journal.action" }.get
    val json = Json.parse(header.value)
    json.as[Action]
  }
}
