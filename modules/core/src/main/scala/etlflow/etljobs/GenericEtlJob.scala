package etlflow.etljobs

import etlflow.log.EtlLogger.JobLoggerImpl
import etlflow.log.{DbLogger, SlackLogger}
import etlflow.utils.{LoggingLevel, UtilityFunctions => UF}
import etlflow.{EtlJobProps, JobEnv, JobLogger, StepEnv, StepLogger}
import zio.{UIO, ZIO, ZLayer}

trait GenericEtlJob[EJP <: EtlJobProps] extends EtlJob[EJP] {

  def job: ZIO[StepEnv, Throwable, Unit]
  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit = {}
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])] = List.empty
  val job_type = "GenericEtlJob"

  final def execute(job_run_id: Option[String] = None, is_master: Option[String] = None): ZIO[JobEnv, Throwable, Unit] = {
    for {
      job_start_time  <- UIO(UF.getCurrentTimestamp)
      jri             = job_run_id.getOrElse(java.util.UUID.randomUUID.toString)
      master_job      = is_master.getOrElse("true")
      slack_env       = config.slack.map(_.env).getOrElse("")
      slack_url       = config.slack.map(_.url).getOrElse("")
      slack           = SlackLogger(job_name, slack_env, slack_url, job_notification_level, job_send_slack_notification)
      db              <- DbLogger(job_name, job_properties, config, jri, master_job, job_notification_level, job_enable_db_logging)
      job_log         = JobLoggerImpl(JobLogger(db.job,slack),job_type)
      step_layer      = ZLayer.succeed(StepLogger(db.step,slack))
      _               <- job_log.logInit(job_start_time)
      _               <- job.provideSomeLayer[JobEnv](step_layer).foldM(
                            ex => job_log.logError(job_start_time,ex),
                            _  => job_log.logSuccess(job_start_time)
                          )
    } yield ()
  }
}
