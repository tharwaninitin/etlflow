package etlflow.audit

import etlflow.log.ApplicationLogger
import zio.{UIO, ULayer, ZIO, ZLayer}

object Console extends ApplicationLogger {

  object ConsoleLogger extends Service[UIO] {
    override val jobRunId: String = ""
    // format: off
    override def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, startTime: Long): UIO[Unit] =
      ZIO.succeed(logger.info(s"Task $taskName started"))
    override def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, endTime: Long, error: Option[Throwable]): UIO[Unit] =
      ZIO.succeed {
        error.fold{
          logger.info(s"Task $taskName completed successfully")
        } { ex => 
          logger.error(s"Task $taskName failed, Error StackTrace:" + "\n" + ex.getStackTrace.mkString("\n"))
        }
      }
    override def logJobStart(jobName: String, args: String, startTime: Long): UIO[Unit] =
      ZIO.succeed(logger.info(s"Job started"))
    override def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): UIO[Unit] =
      ZIO.succeed {
        error.fold {
          logger.info(s"Job completed with success")
        } { ex =>
          logger.info(s"Job completed with failure ${ex.getMessage}")
        }
      }
    // format: on
  }

  val live: ULayer[AuditEnv] = ZLayer.succeed(ConsoleLogger)
}
