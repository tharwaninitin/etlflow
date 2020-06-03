package etlflow.etljobs

import etlflow.utils.{UtilityFunctions => UF}
import zio._

trait GenericEtlJob extends EtlJob {

  val job: Task[Unit]
  def printJobInfo(level: String = "info"): Unit = {}
  def getJobInfo(level: String = "info"): List[(String,Map[String,String])] = List.empty

  final def execute(): ZIO[Any, Throwable, Unit] = {
    for {
      job_status_ref  <- job_status
      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
      _               <- job_status_ref.set("started") *> logJobInit
      _               <- job.foldM(
                            ex => job_status_ref.set(s"failed with error ${ex.getMessage}") *> logJobError(ex,job_start_time),
                            _  => job_status_ref.set("success") *> logJobSuccess(job_start_time)
                          )
    } yield ()
  }

  private val logJobInit: Task[Unit] = Task.succeed(etl_job_logger.info(s"""Starting Job $job_name"""))

  private def logJobError(e: Throwable, job_start_time: Long): Task[Unit] = Task {
    etl_job_logger.error(s"""Job completed with failure ${e.getMessage} in ${UF.getTimeDifferenceAsString(job_start_time, UF.getCurrentTimestamp)}""")
    throw e
  }

  private def logJobSuccess(job_start_time: Long): Task[Unit] = Task {
    etl_job_logger.info(s"Job completed successfully in ${UF.getTimeDifferenceAsString(job_start_time, UF.getCurrentTimestamp)}")
  }
}
