package etlflow.log

import etlflow.utils.ApplicationLogger
import zio.{UIO, ULayer, ZLayer}

object Console extends ApplicationLogger {

  object ConsoleLogger extends Service {
    override val jobRunId: String = ""
    // format: off
    override def logStepStart(stepRunId: String, stepName: String, props: Map[String,String], stepType: String, startTime: Long): UIO[Unit] =
      UIO(logger.info(s"Step $stepName started"))
    override def logStepEnd(stepRunId: String, stepName: String, props: Map[String,String], stepType: String, endTime: Long, error: Option[Throwable]): UIO[Unit] =
      UIO {
        error.fold{
          logger.info(s"Step $stepName completed successfully")
        } { ex => 
          logger.error(s"Step $stepName failed, Error StackTrace:" + "\n" + ex.getStackTrace.mkString("\n"))
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
