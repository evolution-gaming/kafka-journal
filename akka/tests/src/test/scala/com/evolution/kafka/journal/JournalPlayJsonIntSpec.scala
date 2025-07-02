package com.evolution.kafka.journal

import TestJsonCodec.instance
import cats.effect.IO
import cats.syntax.all.*
import com.evolution.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolution.kafka.journal.eventual.EventualRead
import play.api.libs.json.Json

class JournalPlayJsonIntSpec extends JournalIntSpec[Payload] {

  override def event(seqNr: SeqNr): Event[Payload] =
    Event(seqNr, payload = Payload.json(Json.obj("key" -> "value")).some)

  override implicit val kafkaRead: KafkaRead[IO, Payload] = KafkaRead.payloadKafkaRead[IO]
  override implicit val kafkaWrite: KafkaWrite[IO, Payload] = KafkaWrite.payloadKafkaWrite[IO]
  override implicit val eventualRead: EventualRead[IO, Payload] = EventualRead.payloadEventualRead[IO]
}
