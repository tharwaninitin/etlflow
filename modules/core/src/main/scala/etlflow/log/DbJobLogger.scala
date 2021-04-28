package etlflow.log

import doobie.hikari.HikariTransactor
import doobie.implicits._
import etlflow.EtlJobProps
import etlflow.jdbc.CoreSQL
import etlflow.utils.{JsonJackson, UtilityFunctions => UF}
import zio.{Task, UIO}
import zio.interop.catz._

class DbJobLogger(transactor: HikariTransactor[Task], job_name: String, job_properties: EtlJobProps, job_run_id: String, is_master:String) extends ApplicationLogger {
  def logStart(start_time: Long, job_type: String): Task[Unit] = {
    val properties = JsonJackson.convertToJsonByRemovingKeys(job_properties, List.empty)
    logger.info("Logging job start in db")
    CoreSQL
      .insertJobRun(job_run_id, job_name, properties, job_type, is_master, start_time)
      .run.transact(transactor).unit
      .tapError{e =>
        UIO(logger.error(s"failed in logging to db ${e.getMessage}"))
      }
  }
  def logEnd(start_time: Long, error_message: Option[String] = None): Task[Unit] = {
    val job_status = if (error_message.isDefined) "failed with error: " + error_message.get else "pass"
    logger.info(s"Logging job completion in db with status $job_status")
    val elapsed_time = UF.getTimeDifferenceAsString(start_time, UF.getCurrentTimestamp)
    CoreSQL
      .updateJobRun(job_run_id, job_status, elapsed_time)
      .run.transact(transactor).unit
      .tapError{e =>
        UIO(logger.error(s"failed in logging to db ${e.getMessage}"))
      }
  }
}


