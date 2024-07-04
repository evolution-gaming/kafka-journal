package com.evolutiongaming.kafka.journal.circe

import cats.effect.IO
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.circe.Instances.*
import com.evolutiongaming.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolutiongaming.kafka.journal.eventual.EventualRead
import com.evolutiongaming.kafka.journal.{Event, JournalIntSpec, SeqNr}
import io.circe.Json

class JournalCirceIntSpec extends JournalIntSpec[Json] {

  override def event(seqNr: SeqNr): Event[Json] =
    Event(seqNr, payload = Json.obj("key" -> Json.fromString("value")).some)

  override implicit val kafkaRead: KafkaRead[IO, Json]       = Instances.kafkaRead[IO]
  override implicit val kafkaWrite: KafkaWrite[IO, Json]     = Instances.kafkaWrite[IO]
  override implicit val eventualRead: EventualRead[IO, Json] = Instances.eventualRead[IO]
}
