package com.evolutiongaming.kafka.journal

import scala.util.control.NoStackTrace

case object HeadCacheReleasedError extends RuntimeException("HeadCache released") with NoStackTrace