package com.evolution.kafka.journal.circe

import cats.effect.IO
import cats.syntax.all.*
import com.evolution.kafka.journal.circe.Instances.*
import com.evolution.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolution.kafka.journal.eventual.EventualRead
import com.evolution.kafka.journal.{Event, JournalIntSpec, SeqNr}
import io.circe.Json

class JournalCirceIntSpec extends JournalIntSpec[Json] {

  override def event(seqNr: SeqNr): Event[Json] =
    Event(seqNr, payload = Json.obj("key" -> Json.fromString("value")).some)

  override implicit val kafkaRead: KafkaRead[IO, Json] = Instances.kafkaRead[IO]
  override implicit val kafkaWrite: KafkaWrite[IO, Json] = Instances.kafkaWrite[IO]
  override implicit val eventualRead: EventualRead[IO, Json] = Instances.eventualRead[IO]
}
