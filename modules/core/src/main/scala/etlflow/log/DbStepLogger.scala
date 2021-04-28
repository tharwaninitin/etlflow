package etlflow.log

import doobie.hikari.HikariTransactor
import doobie.implicits._
import etlflow.DBEnv
import etlflow.etlsteps.EtlStep
import etlflow.jdbc.{CoreSQL, DbManager}
import etlflow.utils.{JsonJackson, LoggingLevel, UtilityFunctions => UF}
import zio.interop.catz._
import zio.{Task, UIO, URIO, ZIO}

class DbStepLogger(transactor: HikariTransactor[Task], job_run_id: String, job_notification_level: LoggingLevel) extends ApplicationLogger {
  private val remoteStep = List("EtlFlowJobStep","DPSparkJobStep","ParallelETLStep")

  def updateStepLevelInformation(start_time: Long, etl_step: EtlStep[_,_], state_status: String, error_message: Option[String] = None, mode: String = "update"): Task[Unit] = {
    val step_name = UF.stringFormatter(etl_step.name)
    val properties = JsonJackson.convertToJson(etl_step.getStepProperties(job_notification_level))

    if (mode == "insert") {
      val step_run_id = if(remoteStep.contains(etl_step.step_type)) etl_step.getStepProperties(job_notification_level)("step_run_id") else ""
      logger.info(s"Inserting step info for $step_name in db with status => ${state_status.toLowerCase()}")
      CoreSQL
        .insertStepRun(job_run_id, step_name, properties, etl_step.step_type, step_run_id, start_time)
        .run.transact(transactor).unit
        .tapError{e =>
          UIO(logger.error(s"failed in logging to db ${e.getMessage}"))
        }
    }
    else {
      val status = if (error_message.isDefined) state_status.toLowerCase() + " with error: " + error_message.get else state_status.toLowerCase()
      val elapsed_time = UF.getTimeDifferenceAsString(start_time, UF.getCurrentTimestamp)
      logger.info(s"Updating step info for $step_name in db with status => $status")
      CoreSQL
        .updateStepRun(job_run_id, step_name, properties, status, elapsed_time)
        .run.transact(transactor).unit
        .tapError{e =>
          UIO(logger.error(s"failed in logging to db ${e.getMessage}"))
        }
    }
  }
}

object DbStepLogger extends DbManager {
  def apply(job_run_id: String, job_enable_db_logging:Boolean, job_notification_level:LoggingLevel): URIO[DBEnv, Option[DbStepLogger]] = ZIO.access[DBEnv] { x =>
    if (job_enable_db_logging)
      Some(new DbStepLogger(x.get, job_run_id, job_notification_level))
    else
      None
  }
}
