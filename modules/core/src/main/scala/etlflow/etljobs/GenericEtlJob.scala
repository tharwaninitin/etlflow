package etlflow.etljobs

import etlflow._
import etlflow.log.{JobLogger, SlackLogger, StepReq}
import etlflow.schema.LoggingLevel
import etlflow.utils.DateTimeFunctions.getCurrentTimestamp
import zio.{Task, UIO, ZIO, ZLayer}

trait GenericEtlJob[EJP <: EtlJobProps] extends EtlJob[EJP] {

  def job: ZIO[StepEnv, Throwable, Unit]
  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit = {}
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])] = List.empty
  val job_type = "GenericEtlJob"

  final def execute(job_run_id: Option[String] = None, is_master: Option[String] = None, props: String = "{}"): ZIO[JobEnv, Throwable, Unit] = {
    for {
      job_start_time  <- UIO(getCurrentTimestamp)
      jri             = job_run_id.getOrElse(java.util.UUID.randomUUID.toString)
      master_job      = is_master.getOrElse("true")
      slack_env       = config.slack.map(_.env).getOrElse("")
      slack_url       = config.slack.map(_.url).getOrElse("")
      host_url        = config.host.getOrElse("http://localhost:8080/#")  + "/JobRunDetails/" + jri
      slack           = SlackLogger(job_name, slack_env, slack_url, job_notification_level, job_send_slack_notification, host_url)
      dbJob           = new JobLogger(job_name, props, jri, master_job, slack)
      step_layer      = ZLayer.succeed(StepReq(jri, slack))
      _               <- dbJob.logStart(job_start_time, job_type)
      _               <- job.provideSomeLayer[JobEnv](step_layer).foldM(
                            ex => dbJob.logEnd(job_start_time, Some(ex.getMessage)).unit *> Task.fail(ex),
                            _  => dbJob.logEnd(job_start_time)
                          )
    } yield ()
  }
}
