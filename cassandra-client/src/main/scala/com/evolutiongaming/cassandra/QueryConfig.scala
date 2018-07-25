package com.evolutiongaming.cassandra

import com.datastax.driver.core.{ConsistencyLevel, QueryOptions}
import com.evolutiongaming.config.ConfigHelper._
import com.typesafe.config.{Config, ConfigException}

import scala.concurrent.duration._

/**
  * See [[https://docs.datastax.com/en/drivers/java/3.5/com/datastax/driver/core/QueryOptions.html]]
  */
final case class QueryConfig(
  consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_ONE,
  serialConsistency: ConsistencyLevel = ConsistencyLevel.SERIAL,
  fetchSize: Int = 5000,
  defaultIdempotence: Boolean = false,
  maxPendingRefreshNodeListRequests: Int = 20,
  maxPendingRefreshNodeRequests: Int = 20,
  maxPendingRefreshSchemaRequests: Int = 20,
  refreshNodeListInterval: FiniteDuration = 1.second,
  refreshNodeInterval: FiniteDuration = 1.second,
  refreshSchemaInterval: FiniteDuration = 1.second,
  metadata: Boolean = true,
  rePrepareOnUp: Boolean = true,
  prepareOnAllHosts: Boolean = true) {

  def asJava: QueryOptions = {
    new QueryOptions()
      .setConsistencyLevel(consistency)
      .setSerialConsistencyLevel(serialConsistency)
      .setFetchSize(fetchSize)
      .setDefaultIdempotence(defaultIdempotence)
      .setMaxPendingRefreshNodeListRequests(maxPendingRefreshNodeListRequests)
      .setMaxPendingRefreshNodeRequests(maxPendingRefreshNodeRequests)
      .setMaxPendingRefreshSchemaRequests(maxPendingRefreshSchemaRequests)
      .setRefreshNodeListIntervalMillis(refreshNodeListInterval.toMillis.toInt)
      .setRefreshNodeIntervalMillis(refreshNodeInterval.toMillis.toInt)
      .setRefreshSchemaIntervalMillis(refreshSchemaInterval.toMillis.toInt)
      .setMetadataEnabled(metadata)
      .setReprepareOnUp(rePrepareOnUp)
      .setPrepareOnAllHosts(prepareOnAllHosts)
  }
}

object QueryConfig {

  val Default: QueryConfig = QueryConfig()

  private implicit val ConsistencyLevelFromConf = FromConf[ConsistencyLevel] { (conf, path) =>
    val str = conf.getString(path)
    val value = ConsistencyLevel.values().find { _.name equalsIgnoreCase str }
    value getOrElse {
      throw new ConfigException.BadValue(conf.origin(), path, s"Cannot parse ConsistencyLevel from $str")
    }
  }

  def apply(config: Config): QueryConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    QueryConfig(
      consistency = get[ConsistencyLevel]("consistency") getOrElse Default.consistency,
      serialConsistency = get[ConsistencyLevel]("serial-consistency") getOrElse Default.serialConsistency,
      fetchSize = get[Int]("fetch-size") getOrElse Default.fetchSize,
      defaultIdempotence = get[Boolean]("default-idempotence") getOrElse Default.defaultIdempotence,
      maxPendingRefreshNodeListRequests = get[Int]("max-pending-refresh-node-list-requests") getOrElse Default.maxPendingRefreshNodeListRequests,
      maxPendingRefreshNodeRequests = get[Int]("max-pending-refresh-node-requests") getOrElse Default.maxPendingRefreshNodeRequests,
      maxPendingRefreshSchemaRequests = get[Int]("max-pending-refresh-schema-requests") getOrElse Default.maxPendingRefreshSchemaRequests,
      refreshNodeListInterval = get[FiniteDuration]("refresh-node-list-interval") getOrElse Default.refreshNodeListInterval,
      refreshNodeInterval = get[FiniteDuration]("refresh-node-interval") getOrElse Default.refreshNodeInterval,
      refreshSchemaInterval = get[FiniteDuration]("refresh-schema-interval") getOrElse Default.refreshSchemaInterval,
      metadata = get[Boolean]("metadata") getOrElse Default.metadata,
      rePrepareOnUp = get[Boolean]("re-prepare-on-up") getOrElse Default.rePrepareOnUp,
      prepareOnAllHosts = get[Boolean]("prepare-on-all-hosts") getOrElse Default.prepareOnAllHosts)
  }
}
