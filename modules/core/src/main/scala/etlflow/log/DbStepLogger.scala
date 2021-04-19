package etlflow.log

import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.util.meta.Meta
import etlflow.DBEnv
import etlflow.etlsteps.EtlStep
import etlflow.jdbc.DbManager
import etlflow.utils.{JsonJackson, LoggingLevel, UtilityFunctions => UF}
import org.postgresql.util.PGobject
import zio.interop.catz._
import zio.{Task, URIO, ZIO}

class DbStepLogger(transactor: HikariTransactor[Task], job_run_id: String, job_notification_level:LoggingLevel) extends ApplicationLogger {
  private val remoteStep = List("EtlFlowJobStep","DPSparkJobStep","ParallelETLStep")

  implicit val jsonMeta: Meta[JsonString] = Meta.Advanced.other[PGobject]("jsonb").timap[JsonString](o => JsonString(o.getValue))(a => {
    val o = new PGobject
    o.setType("jsonb")
    o.setValue(a.str)
    o
  })

  def updateStepLevelInformation(execution_start_time: Long, etl_step: EtlStep[_,_], state_status: String, error_message: Option[String] = None, mode: String = "update"): Task[Long] = {
    val formatted_step_name = UF.stringFormatter(etl_step.name)
    if (mode == "insert") {
      val step = StepRun(
        job_run_id,
        formatted_step_name,
        JsonJackson.convertToJson(etl_step.getStepProperties(job_notification_level)),
        state_status.toLowerCase(),
        UF.getCurrentTimestampAsString(),
        "...",
        etl_step.step_type,
        if(remoteStep.contains(etl_step.step_type)) etl_step.getStepProperties(job_notification_level)("step_run_id") else ""
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
            VALUES (${step.job_run_id}, ${step.step_name},${JsonString(step.properties)}, ${step.state}, ${step.start_time}, ${step.elapsed_time}, ${step.step_type}, ${step.step_run_id})"""

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
                  properties = ${JsonString(JsonJackson.convertToJson(etl_step.getStepProperties(job_notification_level)))},
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
  def apply(job_run_id: String, job_enable_db_logging:Boolean, job_notification_level:LoggingLevel): URIO[DBEnv, Option[DbStepLogger]] = ZIO.access[DBEnv] { x =>
    if (job_enable_db_logging)
      Some(new DbStepLogger(x.get, job_run_id, job_notification_level))
    else
      None
  }
}
