package com.evolutiongaming.kafka.journal

import scala.util.control.NoStackTrace

private[journal] final case object ReleasedError extends RuntimeException("released") with NoStackTrace
