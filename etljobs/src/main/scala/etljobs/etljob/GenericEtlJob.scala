package etljobs.etljob

import etljobs.utils.{UtilityFunctions => UF}
import zio.{Task, UIO, ZIO}

trait GenericEtlJob extends EtlJob {

  def etl_job: Task[Unit]

  def printJobInfo(level: String = "info"): Unit = {}

  def getJobInfo(level: String = "info"): List[(String,Map[String,String])] = List.empty

  def execute(): Unit = {
      val job = (for {
        job_start_time  <- UIO.succeed(UF.getCurrentTimestamp).toManaged_
        transactor      <- createDbResource(global_properties,platform.executor.asEC)
        db              <- createDbLogger(transactor,job_name,job_properties)
        _               <- logJobInit(Some(db)).toManaged_
        _               <- etl_job.mapError(e => logError(e,job_start_time)(Some(db))).toManaged_
        _               <- logSuccess(job_start_time)(Some(db)).toManaged_
      } yield ()).use_(ZIO.unit)
      runtime.unsafeRun(job)
  }
}
