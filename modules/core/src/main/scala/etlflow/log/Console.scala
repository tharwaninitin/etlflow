package etlflow.log

import etlflow.utils.ApplicationLogger
import zio.{UIO, ULayer, ZLayer}

object Console extends ApplicationLogger {

  object ConsoleLogger extends Service {
    override val job_run_id: String = ""
    // format: off
    override def logStepStart(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, start_time: Long): UIO[Unit] =
      UIO(logger.info(s"Step $step_name started"))
    def logStepEnd(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, end_time: Long, error: Option[Throwable]): UIO[Unit] =
      UIO {
        if (error.isEmpty)
          logger.info(s"Step $step_name completed successfully")
        else
          logger.error(s"Step $step_name failed, Error StackTrace:" + "\n" + error.get.getStackTrace.mkString("\n"))
      }
    override def logJobStart(job_name: String, args: String, start_time: Long): UIO[Unit] =
      UIO(logger.info(s"Job  started"))
    override def logJobEnd(job_name: String, args: String, end_time: Long, error: Option[Throwable]): UIO[Unit] =
      UIO {
        if (error.isEmpty)
          logger.info(s"Job completed with success")
        else
          logger.info(s"Job completed with failure ${error.get.getMessage}")
      }
    // format: on
  }

  val live: ULayer[LogEnv] = ZLayer.succeed(ConsoleLogger)
}
