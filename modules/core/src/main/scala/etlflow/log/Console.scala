package etlflow.log

import etlflow.utils.ApplicationLogger
import zio.{UIO, ULayer, ZLayer}

object Console extends ApplicationLogger {

  object ConsoleLogger extends Service[UIO] {
    override val jobRunId: String = ""
    // format: off
    override def logTaskStart(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, startTime: Long): UIO[Unit] =
      UIO(logger.info(s"Task $taskName started"))
    override def logTaskEnd(taskRunId: String, taskName: String, props: Map[String,String], taskType: String, endTime: Long, error: Option[Throwable]): UIO[Unit] =
      UIO {
        error.fold{
          logger.info(s"Task $taskName completed successfully")
        } { ex => 
          logger.error(s"Task $taskName failed, Error StackTrace:" + "\n" + ex.getStackTrace.mkString("\n"))
        }
      }
    override def logJobStart(jobName: String, args: String, startTime: Long): UIO[Unit] =
      UIO(logger.info(s"Job started"))
    override def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): UIO[Unit] =
      UIO {
        error.fold {
          logger.info(s"Job completed with success")
        } { ex =>
          logger.info(s"Job completed with failure ${ex.getMessage}")
        }
      }
    // format: on
  }

  val live: ULayer[LogEnv] = ZLayer.succeed(ConsoleLogger)
}
