package com.evolutiongaming.hostname

import java.net.InetAddress
import java.util.concurrent.Executors

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.sys.process._
import scala.util.Properties
import scala.util.control.NonFatal

// TODO Move out to separate repository
object HostName {

  def apply(): Option[String] = {

    val os = env("os.name").map(_.toLowerCase)

    {
      if (os contains "win") win()
      else if ((os contains "nix") || (os contains "nux") || (os contains "mac")) unix()
      else unix() orElse win()
    } orElse {
      inetAddress()
    }
  }

  private[hostname] def inetAddress() = {
    val service = Executors.newSingleThreadExecutor()
    implicit val ec = ExecutionContext.fromExecutor(service)
    val future = Future {
      InetAddress.getLocalHost.getHostName
    }
    future.onComplete { _ => service.shutdown() }
    safe { Await.result(future, 1.second) }.filter(str => str != "localhost")
  }

  private[hostname] def win() =
    env("COMPUTERNAME") orElse
      exec("hostname")

  private[hostname] def unix() =
    env("HOSTNAME") orElse
      exec("hostname") orElse
      env("gethostname") orElse
      env("cat /etc/hostname")

  private[hostname] def exec(name: String) = safe { name.!! }

  private[hostname] def env(name: String) = Properties.envOrNone(name)

  private[hostname] def safe(f: => String) = {
    try {
      Option(f.trim).filter(_.nonEmpty)
    } catch {
      case NonFatal(_) => None
    }
  }
}
