package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import scodec.bits.ByteVector

import scala.collection.concurrent.TrieMap

trait FixEquality[A] { self =>

  def apply(a: A): A
}

object FixEquality {

  implicit def payloadImpl(implicit F: FixEquality[Bytes]): FixEquality[Payload] = new FixEquality[Payload] {
    def apply(a: Payload) = a match {
      case a: Payload.Binary => a.copy(value = ByteVector.view(F(a.value.toArray)))
      case a                 => a
    }
  }

  implicit def eventImpl(implicit F: FixEquality[Payload]): FixEquality[Event] = new FixEquality[Event] {
    def apply(a: Event) = a.payload.fold(a) { payload => a.copy(payload = Some(F(payload))) }
  }

  implicit def eventsImpl(implicit F: FixEquality[Event]): FixEquality[Nel[Event]] = new FixEquality[Nel[Event]] {
    def apply(a: Nel[Event]) = a.map(F.apply)
  }

  implicit def eventRecordImpl(implicit F: FixEquality[Event]): FixEquality[EventRecord] = new FixEquality[EventRecord] {
    def apply(a: EventRecord) = a.copy(event = F(a.event))
  }

  implicit def eventRecordsImpl(implicit F: FixEquality[EventRecord]): FixEquality[List[EventRecord]] = new FixEquality[List[EventRecord]] {
    def apply(a: List[EventRecord]) = a.map(F.apply)
  }

  def array[A](): FixEquality[Array[A]] = {
    val cache = TrieMap.empty[List[A], Array[A]]
    new FixEquality[Array[A]] {
      def apply(a: Array[A]) = {
        cache.getOrElseUpdate(a.toList, a)
      }
    }
  }

  def apply[A](implicit F: FixEquality[A]): FixEquality[A] = F

  object Implicits {

    implicit class FixEqualityIdOps[A](val a: A) extends AnyVal {
      def fix(implicit F: FixEquality[A]): A = F(a)
    }
  }
}
