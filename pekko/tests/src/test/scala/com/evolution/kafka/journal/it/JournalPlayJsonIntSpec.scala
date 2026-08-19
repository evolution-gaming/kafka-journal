package com.evolution.kafka.journal.it

import cats.effect.IO
import cats.implicits.*
import com.evolution.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolution.kafka.journal.eventual.EventualRead
import com.evolution.kafka.journal.{Event, Payload, SeqNr}
import play.api.libs.json.Json

class JournalPlayJsonIntSpec extends JournalIntSpec[Payload] {
  import ItInstances.*

  override def event(seqNr: SeqNr): Event[Payload] =
    Event(seqNr, payload = Payload.json(Json.obj("key" -> "value")).some)

  override implicit val kafkaRead: KafkaRead[IO, Payload] = KafkaRead.payloadKafkaRead[IO]
  override implicit val kafkaWrite: KafkaWrite[IO, Payload] = KafkaWrite.payloadKafkaWrite[IO]
  override implicit val eventualRead: EventualRead[IO, Payload] = EventualRead.payloadEventualRead[IO]
}
