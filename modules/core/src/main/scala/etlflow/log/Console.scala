package etlflow.log

import etlflow.utils.ApplicationLogger
import zio.{Task, UIO, ULayer, ZLayer}

object Console extends ApplicationLogger {

  val live: ULayer[LogEnv] = ZLayer.succeed(
    new Service {
      override val job_run_id: String = ""
      override def logStepStart(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, start_time: Long): Task[Unit] =
        UIO(logger.info(s"Step $step_name started"))
      override def logStepEnd(step_run_id: String, step_name: String, props: Map[String,String], step_type: String, end_time: Long, error: Option[Throwable]): Task[Unit] =
        UIO {
          if(error.isEmpty)
            logger.info(s"Step $step_name completed successfully")
          else
            logger.error(s"Step $step_name failed, Error StackTrace:"+"\n" + error.get.getStackTrace.mkString("\n"))
        }
      override def logJobStart(job_name: String, args: String, start_time: Long): Task[Unit] =
        UIO(logger.info(s"Job  started"))
      override def logJobEnd(job_name: String, args: String, end_time: Long, error: Option[Throwable]): Task[Unit] =
        UIO {
          if(error.isEmpty)
            logger.info(s"Job completed with success")
          else
            logger.info(s"Job completed with failure ${error.get.getMessage}")
        }
    }
  )
}