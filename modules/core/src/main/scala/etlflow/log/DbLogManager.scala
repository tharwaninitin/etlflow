package etlflow.log

import cats.effect.Blocker
import doobie.free.connection.ConnectionIO
import doobie.hikari.HikariTransactor
import doobie.implicits._
import etlflow.EtlJobProps
import etlflow.etlsteps.EtlStep
import etlflow.jdbc.DbManager
import etlflow.utils.{Config, JsonJackson}
import zio.interop.catz._
import zio.{Managed, Task}
import scala.concurrent.ExecutionContext
import etlflow.utils.{UtilityFunctions => UF}

class DbLogManager(val transactor: HikariTransactor[Task],val job_name: String, val job_properties: EtlJobProps,job_run_id:String,is_master:String) extends LogManager[Task[Long]] {

  val remoteStep = List("EtlFlowJobStep","DPSparkJobStep","ParallelETLStep")

  def updateStepLevelInformation(
                                  execution_start_time: Long,
                                  etl_step: EtlStep[_,_],
                                  state_status: String,
                                  error_message: Option[String] = None,
                                  mode: String = "update"
                                ): Task[Long] = {

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
      lm_logger.info(s"Inserting step info for ${formatted_step_name} in db with status => ${state_status.toLowerCase()}")
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
        lm_logger.error(s"failed in logging to db ${e.getMessage}")
        e
      }
      y
    }
    else {
      val status = if (error_message.isDefined) state_status.toLowerCase() + " with error: " + error_message.get else state_status.toLowerCase()
      val elapsed_time = UF.getTimeDifferenceAsString(execution_start_time, UF.getCurrentTimestamp)
      lm_logger.info(s"Updating step info for ${formatted_step_name} in db with status => $status")
      sql"""UPDATE StepRun
              SET state = ${status},
                  properties = ${JsonJackson.convertToJson(etl_step.getStepProperties(job_properties.job_notification_level))},
                  elapsed_time = ${elapsed_time}
            WHERE job_run_id = ${job_run_id} AND step_name = ${formatted_step_name}"""
        .update.run.transact(transactor).map(x => x.toLong).mapError{e =>
        lm_logger.error(s"failed in logging to db ${e.getMessage}")
        e
      }
    }
  }

  def updateJobInformation(execution_start_time: Long,status: String, mode: String = "update", job_type:String,error_message: Option[String] = None): Task[Long] = {
    if (mode == "insert") {
      val job = JobRun(
        job_run_id, job_name.toString,
        JsonJackson.convertToJsonByRemovingKeys(job_properties, List.empty),
        "started",
        UF.getCurrentTimestampAsString(),
        "...",
        job_type,
        is_master
      )
      lm_logger.info(s"Inserting job info in db with status => $status")
      sql"""INSERT INTO JobRun (
              job_run_id,
              job_name,
              properties,
              state,
              start_time,
              elapsed_time,
              job_type,
              is_master)
           VALUES (${job.job_run_id}, ${job.job_name}, ${job.properties}, ${job.state}, ${job.start_time}, ${job.elapsed_time}, ${job.job_type}, ${job.is_master})"""
        .update.run.transact(transactor).map(x => x.toLong).mapError{e =>
        lm_logger.error(s"failed in logging to db ${e.getMessage}")
        e
      }
    }
    else {
      lm_logger.info(s"Updating job info in db with status => $status")
      val job_status = if (error_message.isDefined) status.toLowerCase() + " with error: " + error_message.get else status.toLowerCase()
      val elapsed_time = UF.getTimeDifferenceAsString(execution_start_time, UF.getCurrentTimestamp)
      sql""" UPDATE JobRun
                SET state = ${job_status},
                    elapsed_time = ${elapsed_time}
             WHERE job_run_id = ${job_run_id}"""
        .update.run.transact(transactor).map(x => x.toLong).mapError{e =>
        lm_logger.error(s"failed in logging to db ${e.getMessage}")
        e
      }
    }
  }
}

object DbLogManager extends DbManager{

  def createOptionDbTransactorManagedGP(
                                         global_properties: Config,
                                         ec: ExecutionContext,
                                         blocker: Blocker,
                                         pool_name: String = "LoggerPool",
                                         job_name: String,
                                         job_properties: EtlJobProps,
                                         job_run_id:String,
                                         is_master:String

                                       ): Managed[Throwable, Option[DbLogManager]] =
    if (job_properties.job_enable_db_logging) {
      createDbTransactorManaged(global_properties.dbLog,ec,pool_name)(blocker)
        .map { transactor =>
          Some(new DbLogManager(transactor, job_name, job_properties,job_run_id,is_master))
        }
    } else {
      Managed.unit.map(_ => None)
    }
}
