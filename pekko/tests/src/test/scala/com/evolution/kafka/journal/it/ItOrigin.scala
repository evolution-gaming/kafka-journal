package com.evolution.kafka.journal.it

import com.evolution.kafka.journal.Origin
import org.scalatest.Suite

/**
 * Provides [[Origin]] value with the test suite name for integration tests.
 *
 * Could be used to mark the journal data written by integration tests. Which could help to
 * distinguish which data belongs to which test suite.
 */
private[it] trait ItOrigin { this: Suite =>
  protected final def origin: Origin = Origin(suiteName)
}
