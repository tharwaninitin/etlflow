package etlflow.log

import etlflow.db.{DBApi, DBEnv}
import etlflow.etlsteps.EtlStep
import etlflow.json.JsonEnv
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.{getCurrentTimestamp, getTimeDifferenceAsString}
import zio.{RIO, Task, ULayer, ZIO, ZLayer}

object Implementation extends ApplicationLogger{

  def live(slack: Option[SlackLogger] = None): ULayer[LoggerEnv] = ZLayer.succeed(
    new LoggerApi.Service {
      var job_run_id : String = null
      override def setJobRunId(jri: => String): Task[Unit] = Task{job_run_id = jri}
      override def jobLogStart(start_time: Long, job_type: String, job_name:String, props:String, is_master:String ):
      RIO[LoggerEnv with DBEnv with JsonEnv,Unit] = {
        for{
          _  <- Task(logger.info("Logging job start in db"))
          _  <- DBApi.insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time)
        } yield ()
      }

      override def jobLogEnd(start_time: Long, error_message: Option[String]):
      RIO[LoggerEnv with DBEnv with JsonEnv,Unit] = {
        var job_status = ""
        if (error_message.isDefined) {
          slack.foreach(_.logJobEnd(start_time, error_message))
          job_status = "failed with error: " + error_message.get
        } else {
          slack.foreach(_.logJobEnd(start_time))
          job_status = "pass"
        }
        logger.info(s"Logging job completion in db with status $job_status")
        val elapsed_time = getTimeDifferenceAsString(start_time, getCurrentTimestamp)
        DBApi.updateJobRun(job_run_id, job_status, elapsed_time)
      }

      override def stepLogInit(start_time: Long, etlStep: EtlStep[_,_]):
      RIO[LoggerEnv with DBEnv with JsonEnv,Unit] = {
        val stepLogger = new StepLogger(etlStep, job_run_id)
        stepLogger.update(start_time, "started", mode = "insert")
      }

      override def stepLogSuccess(start_time: Long, etlStep: EtlStep[_,_])
      : RIO[LoggerEnv with DBEnv with JsonEnv with DBEnv with JsonEnv,Unit] = {
        val stepLogger = new StepLogger(etlStep, job_run_id)
        slack.foreach(_.logStepEnd(start_time, etlStep))
        stepLogger.update(start_time, "pass")
      }

      override def stepLogError(start_time: Long, ex: Throwable, etlStep: EtlStep[_,_]):
      ZIO[LoggerEnv with DBEnv with JsonEnv, Throwable, Unit]= {
        val stepLogger = new StepLogger(etlStep, job_run_id)
        logger.error("Step Error StackTrace:"+"\n"+ex.getStackTrace.mkString("\n"))
        slack.foreach(_.logStepEnd(start_time, etlStep, Some(ex.getMessage)))
        stepLogger.update(start_time, "failed", Some(ex.getMessage)) *> Task.fail(new RuntimeException(ex.getMessage))
      }
    }
  )
}
