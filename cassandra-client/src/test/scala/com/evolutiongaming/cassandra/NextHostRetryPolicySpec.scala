package com.evolutiongaming.cassandra

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.scalatest.{Matchers, WordSpec}

class NextHostRetryPolicySpec extends WordSpec with Matchers {

  "NextHostRetryPolicy" should {

    val statement = QueryBuilder.select().all().from("")
    val consistency = ConsistencyLevel.ALL

    for {
      (retry, expected) <- List(
        (0, RetryDecision.Type.RETRY),
        (1, RetryDecision.Type.RETHROW),
        (2, RetryDecision.Type.RETHROW))
    } {

      s"onWriteTimeout, retry: $retry, expected: $expected" in {
        val policy = NextHostRetryPolicy(1)
        val decision = policy.onWriteTimeout(statement, consistency, WriteType.SIMPLE, 1, 1, retry)
        decision.getType shouldEqual expected
      }

      s"onReadTimeout, retry: $retry, expected: $expected" in {
        val policy = NextHostRetryPolicy(1)
        val decision = policy.onReadTimeout(statement, consistency, 1, 1, false, retry)
        decision.getType shouldEqual expected
      }

      s"onRequestError, retry: $retry, expected: $expected" in {
        val policy = NextHostRetryPolicy(1)
        val decision = policy.onRequestError(statement, consistency, new DriverException(""), retry)
        decision.getType shouldEqual expected
      }

      s"onUnavailable, retry: $retry, expected: $expected" in {
        val policy = NextHostRetryPolicy(1)
        val decision = policy.onUnavailable(statement, consistency, 1, 1, retry)
        decision.getType shouldEqual expected
      }
    }
  }
}
