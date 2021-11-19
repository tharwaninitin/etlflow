package etlflow.log

import etlflow.db.{DBApi, DBEnv}
import etlflow.etlsteps.EtlStep
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.LoggingLevel
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.{getCurrentTimestamp, getTimeDifferenceAsString}
import zio.{RIO, Task, UIO, ULayer, ZIO, ZLayer}

object Implementation extends ApplicationLogger {

  private class StepLogger(etlStep: EtlStep[_, _], job_run_id: String, job_notification_level: LoggingLevel = LoggingLevel.INFO) extends ApplicationLogger {

    val remoteStep = List("EtlFlowJobStep", "DPSparkJobStep", "ParallelETLStep")

    private def stringFormatter(value: String): String =
      value.take(50).replaceAll("[^a-zA-Z0-9]", " ").replaceAll("\\s+", "_").toLowerCase

    def update(start_time: Long, state_status: String, error_message: Option[String] = None, mode: String = "update"): ZIO[DBEnv with JsonEnv, Throwable, Unit] = {
      val step_name = stringFormatter(etlStep.name)
      if (mode == "insert") {
        val step_run_id = if (remoteStep.contains(etlStep.step_type)) etlStep.getStepProperties(job_notification_level)("step_run_id") else ""
        for {
          properties <- JsonApi.convertToString(etlStep.getStepProperties(job_notification_level), List.empty)
          _ <- DBApi.insertStepRun(job_run_id, step_name, properties, etlStep.step_type, step_run_id, start_time)
        } yield ()
      }
      else {
        val status = if (error_message.isDefined) state_status.toLowerCase() + " with error: " + error_message.get else state_status.toLowerCase()
        val elapsed_time = getTimeDifferenceAsString(start_time, getCurrentTimestamp)
        for {
          properties <- JsonApi.convertToString(etlStep.getStepProperties(job_notification_level), List.empty)
          _ <- DBApi.updateStepRun(job_run_id, step_name, properties, status, elapsed_time)
        } yield ()
      }
    }
  }

  val live: ULayer[LoggerEnv] = ZLayer.succeed(

    new LoggerApi.Service {

      var job_run_id: String = null

      override def setJobRunId(jri: String): UIO[Unit] = UIO {
        job_run_id = jri
      }

      override def jobLogStart(start_time: Long, job_type: String, job_name: String, props: String, is_master: String): RIO[DBEnv with ConsoleEnv, Unit] = {
        ConsoleApi.jobLogStart *>
          DBApi.insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time)
      }

      override def jobLogEnd(start_time: Long, job_run_id: String, job_name: String, ex: Option[Throwable]): RIO[DBEnv with ConsoleEnv with SlackEnv, Unit] = {
        val elapsed_time = getTimeDifferenceAsString(start_time, getCurrentTimestamp)
        val status = if (ex.isEmpty) "pass" else "failed with error: " + ex.get.getMessage
        val console_status = if (ex.isEmpty) None else Some(status)
        val slack_status = if (ex.isEmpty) None else Some(ex.get.getMessage)
        ConsoleApi.jobLogEnd(console_status) *>
          SlackApi.logJobEnd(job_name, job_run_id, LoggingLevel.INFO, start_time, slack_status) *>
          DBApi.updateJobRun(job_run_id, status, elapsed_time) *>
          (if (ex.isEmpty) ZIO.unit else Task.fail(new RuntimeException(ex.get.getMessage)))
      }

      override def stepLogStart(start_time: Long, etlStep: EtlStep[_, _]): RIO[DBEnv with ConsoleEnv with JsonEnv, Unit] = {
        val stepLogger = new StepLogger(etlStep, job_run_id)
        ConsoleApi.stepLogStart(etlStep.name) *>
          stepLogger.update(start_time, "started", mode = "insert")
      }

      override def stepLogEnd(start_time: Long, etlStep: EtlStep[_, _], ex: Option[Throwable]): RIO[DBEnv with ConsoleEnv with SlackEnv with JsonEnv, Unit] = {
        val stepLogger = new StepLogger(etlStep, job_run_id)
        val status = if (ex.isEmpty) "pass" else "failed" + ex.get.getMessage
        val error = if (ex.isEmpty) None else Some(ex.get.getMessage)
        ConsoleApi.stepLogEnd(etlStep.name, if (ex.isEmpty) None else Some(ex.get.getStackTrace.mkString("\n"))) *>
          SlackApi.logStepEnd(start_time, LoggingLevel.INFO, etlStep) *>
          stepLogger.update(start_time, status, error) *>
          (if (ex.isEmpty) ZIO.unit else Task.fail(new RuntimeException(ex.get.getMessage)))
      }
    }
  )
}
