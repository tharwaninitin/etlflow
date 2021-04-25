package etlflow.coretests.log

import etlflow._
import etlflow.jdbc.DbManager
import etlflow.log.EtlLogger.JobLoggerImpl
import etlflow.log.{DbLogger, SlackLogger}
import etlflow.utils.{Configuration, LoggingLevel, UtilityFunctions => UF}
import zio.{UIO, ZEnv, ZIO, ZLayer}

object JobExecutor extends Configuration with DbManager {
  def apply(job_name: String, slack_env: String, slack_url: String, job: ZIO[StepEnv, Throwable, Unit], job_notification_level:LoggingLevel, job_send_slack_notification:Boolean, host_url:String)
  : ZIO[ZEnv, Throwable, SlackLogger] = {
    val slack = for {
      job_start_time  <- UIO.succeed(UF.getCurrentTimestamp)
      jri             = java.util.UUID.randomUUID.toString
      master_job      = "true"
      db              <- DbLogger(job_name, new EtlJobProps{}, config, jri, master_job, job_notification_level, job_enable_db_logging = true)
      slack           = SlackLogger(job_name, slack_env, slack_url, job_notification_level, job_send_slack_notification,host_url)
      job_log         = JobLoggerImpl(JobLogger(db.job,slack),"")
      step_layer      = ZLayer.succeed(StepLogger(db.step,slack))
      _               <- job_log.logInit(job_start_time)
      _               <- job.provideSomeLayer[JobEnv](step_layer).foldM(
                            ex => job_log.logError(job_start_time,ex).orElse(ZIO.unit),
                            _  => job_log.logSuccess(job_start_time)
                         )
    } yield slack.get
    slack.provideCustomLayer(liveTransactor(config.dbLog))
  }
}
