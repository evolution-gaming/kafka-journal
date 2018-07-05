package com.evolutiongaming.cassandra

import java.io.File

import com.evolutiongaming.tmpdir.TmpDir
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.service.CassandraDaemon

import scala.util.Properties

object StartCassandra {

  type Shutdown = () => Unit

  object Shutdown {
    val Empty: Shutdown = () => ()
  }

  def apply(): Shutdown = {

    val tmpDir = TmpDir("cassandra-")

    val trigger = {
      val file = new File(tmpDir.file, "triggers")
      if (!file.exists()) file.mkdir()
      file
    }

    val props = Map(
      ("cassandra.config.loader", "com.evolutiongaming.cassandra.ConfigLoader"),
      ("cassandra-foreground", "true"),
      ("cassandra.native.epoll.enabled", "false"),
      ("cassandra.unsafesystem", "true"),
      ("com.datastax.driver.FORCE_NIO", "true"),
      ("cassandra.triggers_dir", trigger.getAbsolutePath))

    props.foreach { case (k, v) => Properties.setProp(k, v) }

    val config = ServerConfig(tmpDir.file.getAbsolutePath)
    ConfigLoader.config = config.asJava
    DatabaseDescriptor.daemonInitialization()
    DatabaseDescriptor.createAllDirectories()

    val daemon = new CassandraDaemon(true)
    daemon.activate()
    () => daemon.deactivate()
  }
}
