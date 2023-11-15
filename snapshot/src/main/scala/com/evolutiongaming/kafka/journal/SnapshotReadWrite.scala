package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.eventual.EventualRead
import com.evolutiongaming.kafka.journal.eventual.EventualWrite

final case class SnapshotReadWrite[F[_], A](eventualRead: EventualRead[F, A], eventualWrite: EventualWrite[F, A])
