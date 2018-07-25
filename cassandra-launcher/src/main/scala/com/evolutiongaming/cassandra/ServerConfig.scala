package com.evolutiongaming.cassandra

import org.apache.cassandra.config.Config._
import org.apache.cassandra.config.{Config, ParameterizedClass}

import scala.collection.JavaConverters._


final case class ServerConfig(
  dir: String,
  clusterName: String = "Test Cluster",
  address: String = "localhost",
  port: Int = 9042) {

  def asJava: Config = new Config {
    cluster_name = clusterName
    authenticator = "AllowAllAuthenticator"
    authorizer = "AllowAllAuthorizer"
    role_manager = "CassandraRoleManager"
    partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
    hints_directory = s"$dir/hints"
    seed_provider = new ParameterizedClass("org.apache.cassandra.locator.SimpleSeedProvider", Map("seeds" -> "127.0.0.1").asJava)
    disk_access_mode = DiskAccessMode.mmap
    disk_failure_policy = DiskFailurePolicy.stop
    num_tokens = 256
    memtable_flush_writers = 2
    memtable_heap_space_in_mb = 910
    memtable_offheap_space_in_mb = 910
    listen_address = address
    start_rpc = false
    rpc_address = address
    start_native_transport = true
    concurrent_compactors = 2
    data_file_directories = Array(s"$dir/data")
    saved_caches_directory = s"$dir/saved_caches"
    commitlog_directory = s"$dir/commitlog"
    commitlog_total_space_in_mb = 8192
    commitlog_sync = CommitLogSync.periodic
    commitlog_sync_period_in_ms = 10000
    max_mutation_size_in_kb = 16384
    cdc_raw_directory = s"$dir/cdc_raw"
    cdc_total_space_in_mb = 4096
    endpoint_snitch = "SimpleSnitch"
    request_scheduler = "org.apache.cassandra.scheduler.NoScheduler"
    internode_compression = InternodeCompression.none
    file_cache_size_in_mb = 512
    file_cache_round_up = false
    inter_dc_tcp_nodelay = false
    gc_warn_threshold_in_ms = 1000
    windows_timer_interval = 1
    native_transport_port = port
  }
}