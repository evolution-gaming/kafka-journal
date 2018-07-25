package com.evolutiongaming.cassandra

import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.{Cluster => ClusterJ, ConsistencyLevel, Statement, WriteType}

final case class NextHostRetryPolicy(retries: Int) extends RetryPolicy {

  def onUnavailable(
    statement: Statement,
    consistencyLevel: ConsistencyLevel,
    requiredReplica: Int,
    aliveReplica: Int,
    retryNr: Int): RetryDecision = {

    if (retryNr == 0) tryNextHost(consistencyLevel, retryNr)
    else retry(consistencyLevel, retryNr)
  }

  def onWriteTimeout(
    statement: Statement,
    consistencyLevel: ConsistencyLevel,
    writeType: WriteType,
    requiredAcks: Int,
    receivedAcks: Int,
    retryNr: Int): RetryDecision = {

    retry(consistencyLevel, retryNr)
  }

  def onReadTimeout(
    statement: Statement,
    consistencyLevel: ConsistencyLevel,
    requiredResponses: Int,
    receivedResponses: Int,
    dataRetrieved: Boolean,
    retryNr: Int): RetryDecision = {

    retry(consistencyLevel, retryNr)
  }
  def onRequestError(
    statement: Statement,
    consistencyLevel: ConsistencyLevel,
    cause: DriverException,
    nbRetry: Int): RetryDecision = {

    tryNextHost(consistencyLevel, nbRetry)
  }

  def init(cluster: ClusterJ): Unit = {}

  def close(): Unit = {}

  private def retry(consistencyLevel: ConsistencyLevel, retryNr: Int): RetryDecision = {
    if (retryNr < retries) RetryDecision.retry(consistencyLevel)
    else RetryDecision.rethrow()
  }

  private def tryNextHost(consistencyLevel: ConsistencyLevel, retryNr: Int): RetryDecision = {
    if (retryNr < retries) RetryDecision.tryNextHost(consistencyLevel)
    else RetryDecision.rethrow()
  }
}
