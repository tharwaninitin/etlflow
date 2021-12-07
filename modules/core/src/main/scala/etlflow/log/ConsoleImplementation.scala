package etlflow.log

import etlflow.utils.ApplicationLogger
import zio.{UIO, ULayer, ZIO, ZLayer}

object ConsoleImplementation extends ApplicationLogger {

  val nolog: ULayer[ConsoleLogEnv] = ZLayer.succeed(
    new ConsoleApi.Service {
      override def jobLogStart: UIO[Unit] = ZIO.unit
      override def jobLogEnd(error: Option[String]): UIO[Unit] = ZIO.unit
      override def stepLogStart(name: String): UIO[Unit] = ZIO.unit
      override def stepLogEnd(name: String, error: Option[String]): UIO[Unit] = ZIO.unit
    }
  )

  val live: ULayer[ConsoleLogEnv] = ZLayer.succeed(
    new ConsoleApi.Service {
      override def jobLogStart: UIO[Unit] = UIO(logger.info("Job started"))
      override def jobLogEnd(error: Option[String]): UIO[Unit] = UIO {
        if(error.isEmpty)
          logger.info(s"Job completed with success")
        else
          logger.info(s"Job completed with failure ${error.get}")
      }
      override def stepLogStart(name: String): UIO[Unit] = UIO(logger.info(s"Step $name started"))
      override def stepLogEnd(name: String, error: Option[String]): UIO[Unit] = UIO {
        if(error.isEmpty)
          logger.info(s"Step $name completed successfully")
        else
          logger.error(s"Step $name failed, Error StackTrace:"+"\n" + error.get)
      }
    }
  )
}