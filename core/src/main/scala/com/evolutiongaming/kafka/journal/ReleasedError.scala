package com.evolutiongaming.kafka.journal

import scala.util.control.NoStackTrace

case object ReleasedError extends RuntimeException("released") with NoStackTrace
