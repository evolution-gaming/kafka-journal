package org.apache.zookeeper.server

import java.util.concurrent.CountDownLatch

object SetShutdownHandler {

  def apply(server: ZooKeeperServer): Unit = {
    val countDownLatch = new CountDownLatch(1)
    val handler = new ZooKeeperServerShutdownHandler(countDownLatch)
    server.registerServerShutdownHandler(handler)
  }
}
