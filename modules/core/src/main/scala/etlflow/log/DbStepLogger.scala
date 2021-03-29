package etlflow.log

import cats.effect.Blocker
import doobie.hikari.HikariTransactor
import doobie.implicits._
import etlflow.EtlJobProps
import etlflow.etlsteps.EtlStep
import etlflow.jdbc.DbManager
import etlflow.utils.{Config, JsonJackson, UtilityFunctions => UF}
import zio.interop.catz._
import zio.{Managed, Task}
import scala.concurrent.ExecutionContext

class DbStepLogger(transactor: HikariTransactor[Task], job_properties: EtlJobProps, job_run_id: String) extends ApplicationLogger {
  private val remoteStep = List("EtlFlowJobStep","DPSparkJobStep","ParallelETLStep")
  def updateStepLevelInformation(execution_start_time: Long, etl_step: EtlStep[_,_], state_status: String, error_message: Option[String] = None, mode: String = "update"): Task[Long] = {
    val formatted_step_name = UF.stringFormatter(etl_step.name)
    if (mode == "insert") {
      val step = StepRun(
        job_run_id,
        formatted_step_name,
        JsonJackson.convertToJson(etl_step.getStepProperties(job_properties.job_notification_level)),
        state_status.toLowerCase(),
        UF.getCurrentTimestampAsString(),
        "...",
        etl_step.step_type,
        if(remoteStep.contains(etl_step.step_type)) etl_step.getStepProperties(job_properties.job_notification_level)("step_run_id") else ""
      )
      logger.info(s"Inserting step info for ${formatted_step_name} in db with status => ${state_status.toLowerCase()}")
      val x = sql"""INSERT INTO StepRun (
              job_run_id,
              step_name,
              properties,
              state,
              start_time,
              elapsed_time,
              step_type,
              step_run_id)
            VALUES (${step.job_run_id}, ${step.step_name}, ${step.properties}, ${step.state}, ${step.start_time}, ${step.elapsed_time}, ${step.step_type}, ${step.step_run_id})"""

      val y: Task[Long] = x.update.run.transact(transactor).map(x =>x.toLong).mapError{e =>
        logger.error(s"failed in logging to db ${e.getMessage}")
        e
      }
      y
    }
    else {
      val status = if (error_message.isDefined) state_status.toLowerCase() + " with error: " + error_message.get else state_status.toLowerCase()
      val elapsed_time = UF.getTimeDifferenceAsString(execution_start_time, UF.getCurrentTimestamp)
      logger.info(s"Updating step info for ${formatted_step_name} in db with status => $status")
      sql"""UPDATE StepRun
              SET state = ${status},
                  properties = ${JsonJackson.convertToJson(etl_step.getStepProperties(job_properties.job_notification_level))},
                  elapsed_time = ${elapsed_time}
            WHERE job_run_id = ${job_run_id} AND step_name = ${formatted_step_name}"""
        .update.run.transact(transactor).map(x => x.toLong).mapError{e =>
        logger.error(s"failed in logging to db ${e.getMessage}")
        e
      }
    }
  }
}

object DbStepLogger extends DbManager {
  def apply(config: Config, ec: ExecutionContext, blocker: Blocker, pool_name: String = "LoggerPool", job_name: String, job_properties: EtlJobProps, job_run_id:String, is_master:String): Managed[Throwable, Option[DbStepLogger]] =
    if (job_properties.job_enable_db_logging)
      createDbTransactorManaged(config.dbLog,ec,pool_name)(blocker).map { transactor =>
        Some(new DbStepLogger(transactor, job_properties, job_run_id))
      }
    else
      Managed.unit.map(_ => None)
}
