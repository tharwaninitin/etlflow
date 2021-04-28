package etlflow.log

import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.util.meta.Meta
import etlflow.EtlJobProps
import etlflow.utils.{JsonJackson, UtilityFunctions => UF}
import org.postgresql.util.PGobject
import zio.{Task, UIO}
import zio.interop.catz._

class DbJobLogger(transactor: HikariTransactor[Task], job_name: String, job_properties: EtlJobProps, job_run_id: String, is_master:String) extends ApplicationLogger {

  implicit val jsonMeta: Meta[JsonString] = Meta.Advanced.other[PGobject]("jsonb").timap[JsonString](o => JsonString(o.getValue))(a => {
    val o = new PGobject
    o.setType("jsonb")
    o.setValue(a.str)
    o
  })

  def logStart(start_time: Long, job_type: String): Task[Unit] = {
    val properties = JsonJackson.convertToJsonByRemovingKeys(job_properties, List.empty)
    logger.info("Logging job start in db")
    sql"""INSERT INTO JobRun(
            job_run_id,
            job_name,
            properties,
            state,
            elapsed_time,
            job_type,
            is_master,
            inserted_at
            )
         VALUES ($job_run_id, $job_name, ${JsonString(properties)}, 'started', '...', $job_type, $is_master, $start_time)"""
      .update
      .run
      .transact(transactor).unit
      .tapError{e =>
        UIO(logger.error(s"failed in logging to db ${e.getMessage}"))
      }
  }
  def logEnd(start_time: Long, error_message: Option[String] = None): Task[Unit] = {
    val job_status = if (error_message.isDefined) "failed with error: " + error_message.get else "pass"
    logger.info(s"Logging job completion in db with status $job_status")
    val elapsed_time = UF.getTimeDifferenceAsString(start_time, UF.getCurrentTimestamp)
    sql""" UPDATE JobRun
              SET state = $job_status,
                  elapsed_time = $elapsed_time
           WHERE job_run_id = $job_run_id"""
      .update
      .run
      .transact(transactor).unit
      .tapError{e =>
        UIO(logger.error(s"failed in logging to db ${e.getMessage}"))
      }
  }
}


