package etlflow.log

import etlflow.db.{DBApi, DBEnv}
import etlflow.etlsteps.EtlStep
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.LoggingLevel
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.{getCurrentTimestamp, getTimeDifferenceAsString}
import zio.ZIO

private[etlflow] class StepLogger(etlStep: EtlStep[_,_], job_run_id: String, job_notification_level: LoggingLevel = LoggingLevel.INFO) extends ApplicationLogger {

  val remoteStep = List("EtlFlowJobStep", "DPSparkJobStep", "ParallelETLStep")

  private def stringFormatter(value: String): String =
    value.take(50).replaceAll("[^a-zA-Z0-9]", " ").replaceAll("\\s+", "_").toLowerCase

  def update(start_time: Long, state_status: String, error_message: Option[String] = None, mode: String = "update"): ZIO[DBEnv with JsonEnv, Throwable, Unit] =
  {
    val step_name = stringFormatter(etlStep.name)
    if (mode == "insert") {
      val step_run_id = if (remoteStep.contains(etlStep.step_type)) etlStep.getStepProperties(job_notification_level)("step_run_id") else ""
      for{
        properties <- JsonApi.convertToString(etlStep.getStepProperties(job_notification_level),List.empty)
        _          <- DBApi.insertStepRun(job_run_id, step_name, properties, etlStep.step_type, step_run_id, start_time)
      } yield ()
    }
    else {
      val status = if (error_message.isDefined) state_status.toLowerCase() + " with error: " + error_message.get else state_status.toLowerCase()
      val elapsed_time = getTimeDifferenceAsString(start_time, getCurrentTimestamp)
      for{
        properties <- JsonApi.convertToString(etlStep.getStepProperties(job_notification_level),List.empty)
        _          <- DBApi.updateStepRun(job_run_id, step_name, properties, status, elapsed_time)
      } yield ()
    }
  }
}

