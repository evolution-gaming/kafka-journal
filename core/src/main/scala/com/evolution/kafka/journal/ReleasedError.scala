package com.evolution.kafka.journal

import scala.util.control.NoStackTrace

private[journal] case object ReleasedError extends RuntimeException("released") with NoStackTrace
