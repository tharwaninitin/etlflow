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

  def jobLogStart: URIO[ConsoleEnv, Unit] = ZIO.accessM[ConsoleEnv](_.get.jobLogStart)
  def jobLogEnd(error: Option[String]): URIO[ConsoleEnv, Unit] = ZIO.accessM[ConsoleEnv](_.get.jobLogEnd(error))
  def stepLogStart(name: String): URIO[ConsoleEnv, Unit] = ZIO.accessM[ConsoleEnv](_.get.stepLogStart(name))
  def stepLogEnd(name: String, error: Option[String]): URIO[ConsoleEnv, Unit] = ZIO.accessM[ConsoleEnv](_.get.stepLogEnd(name,error))
  }