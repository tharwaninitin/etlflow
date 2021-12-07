package etlflow.log

import etlflow.utils.ApplicationLogger
import zio.{UIO, URIO, ZIO}

object ConsoleApi extends ApplicationLogger {
  trait Service {
    def jobLogStart: UIO[Unit]
    def jobLogEnd(error: Option[String]): UIO[Unit]
    def stepLogStart(name: String): UIO[Unit]
    def stepLogEnd(name: String, error: Option[String]): UIO[Unit]
  }

  def jobLogStart: URIO[ConsoleLogEnv, Unit] = ZIO.accessM[ConsoleLogEnv](_.get.jobLogStart)
  def jobLogEnd(error: Option[String]): URIO[ConsoleLogEnv, Unit] = ZIO.accessM[ConsoleLogEnv](_.get.jobLogEnd(error))
  def stepLogStart(name: String): URIO[ConsoleLogEnv, Unit] = ZIO.accessM[ConsoleLogEnv](_.get.stepLogStart(name))
  def stepLogEnd(name: String, error: Option[String]): URIO[ConsoleLogEnv, Unit] = ZIO.accessM[ConsoleLogEnv](_.get.stepLogEnd(name,error))
  }