package etlflow.etljobs

import etlflow.log.EtlLogger.JobLoggerEnv
import etlflow.log.{DbLogger, SlackLogger}
import etlflow.utils.{LoggingLevel, UtilityFunctions => UF}
import etlflow.{EtlJobProps, JobLogger, StepEnv, StepLogger, TransactorEnv}
import zio.{UIO, ZEnv, ZIO, ZLayer}

trait GenericEtlJob[EJP <: EtlJobProps] extends EtlJob[EJP] {

  def job: ZIO[StepEnv, Throwable, Unit]
  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit = {}
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])] = List.empty
  val job_type = "GenericEtlJob"

  final def execute(job_run_id: Option[String] = None, is_master: Option[String] = None): ZIO[ZEnv with TransactorEnv, Throwable, Unit] = {
    for {
      job_start_time  <- UIO(UF.getCurrentTimestamp)
      jri             = job_run_id.getOrElse(java.util.UUID.randomUUID.toString)
      master_job      = is_master.getOrElse("true")
      slack           = SlackLogger(job_name, config.slack.map(_.env).getOrElse(""), config.slack.map(_.url).getOrElse(""),job_notification_level,job_send_slack_notification)
      db              <- DbLogger(job_name, job_properties, config, jri, master_job, job_notification_level, job_enable_db_logging)
      job_log         = JobLoggerEnv.live(JobLogger(db.job,slack),job_type)
      step_layer      = ZLayer.succeed(StepLogger(db.step,slack))
      _               <- job_log.logInit(job_start_time)
      _               <- job.provideCustomLayer(step_layer).foldM(
                            ex => job_log.logError(job_start_time,ex),
                            _  => job_log.logSuccess(job_start_time)
                          )
    } yield ()
  }
}
