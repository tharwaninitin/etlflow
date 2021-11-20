package etlflow.executor

import etlflow.etljobs.SequentialEtlJob
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.log.{ConsoleImplementation, SlackImplementation}
import etlflow.schema.Slack
import etlflow.utils.{ReflectAPI => RF}
import etlflow.{CoreEnv, EJPMType}
import zio._

case class LocalExecutor[T <: EJPMType : Tag]() {

  def executeJob(name: String, properties: Map[String, String], slack: Option[Slack] = None, job_run_id: Option[String] = None, is_master: Option[String] = None): ZIO[CoreEnv, Throwable, Unit] = {
    for {
      ejpm <- RF.getJob[T](name)
      job  = ejpm.etlJob(properties)
      _    = {
                job.job_name = ejpm.toString
                job.job_enable_db_logging = ejpm.job_enable_db_logging
                job.job_send_slack_notification = ejpm.job_send_slack_notification
                job.job_notification_level = ejpm.job_notification_level
            }
      props <- ejpm.getActualPropertiesAsJson(properties)
      _     <- job.execute(job_run_id, is_master, props).provideSomeLayer[CoreEnv](SlackImplementation.live(slack) ++ ConsoleImplementation.live)
    } yield ()
  }

  private[etlflow] def showJobProps(name: String): ZIO[JsonEnv, Throwable, Unit] = {
    val exclude_keys = List("job_run_id","job_description","job_properties")
    for {
      ejpm      <- RF.getJob[T](name)
      job_props = ejpm.getProps -- exclude_keys
      _         = println(job_props)
    } yield ()
  }

  private[etlflow] def getActualJobProps(name: String, properties: Map[String, String]): ZIO[JsonEnv, Throwable, String] = {
    for {
      ejpm      <- RF.getJob[T](name)
      job_props <- ejpm.getActualPropertiesAsJson(properties)
    } yield (job_props)
  }

  private[etlflow] def showJobStepProps(name: String, properties: Map[String, String]): ZIO[JsonEnv, Throwable, Unit] = {
    for {
      ejpm     <- RF.getJob[T](name)
      etl_job  = ejpm.etlJob(properties)
      _        <- if (etl_job.isInstanceOf[SequentialEtlJob[_]]) {
                    etl_job.job_name = ejpm.toString
                    JsonApi.convertToString(etl_job.getJobInfo(ejpm.job_notification_level), List.empty).map(println(_))
                  }
                  else {
                    UIO(println("Step Props info not available for generic jobs"))
                  }
    } yield ()
  }
}