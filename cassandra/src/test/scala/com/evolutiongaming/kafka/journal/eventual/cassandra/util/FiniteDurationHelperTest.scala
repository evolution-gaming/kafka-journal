package com.evolutiongaming.kafka.journal.eventual.cassandra.util


import scala.concurrent.duration._
import com.datastax.driver.core.{Duration => DurationC}
import com.evolutiongaming.kafka.journal.eventual.cassandra.util.FiniteDurationHelper._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FiniteDurationHelperTest extends AnyFunSuite with Matchers {

  for {
    (finiteDuration, duration) <- List(
      (1.millis   , DurationC.newInstance(0, 0, 1000000)),
      (100.minutes, DurationC.newInstance(0, 0,  6000000000000L)),
      (30.days    , DurationC.newInstance(0, 30, 0)))
  } {

    test(s"$finiteDuration to cassandra Duration") {
      finiteDurationToDuration(finiteDuration) shouldEqual duration
    }

    test(s"$duration to FiniteDuration") {
      durationToFiniteDuration(duration) shouldEqual finiteDuration
    }

    test(s"$finiteDuration to & from cassandra Duration") {
      durationToFiniteDuration(finiteDurationToDuration(finiteDuration)) shouldEqual finiteDuration
    }
  }
}
