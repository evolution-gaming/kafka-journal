package com.evolutiongaming.kafka.journal

import scala.util.control.NoStackTrace

final case object ReleasedError extends RuntimeException("released") with NoStackTrace
