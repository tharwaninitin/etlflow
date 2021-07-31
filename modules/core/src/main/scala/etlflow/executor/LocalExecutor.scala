package etlflow.executor

import etlflow.etljobs.SequentialEtlJob
import etlflow.json.{JsonApi, JsonEnv}
import etlflow.schema.Config
import etlflow.utils.{ReflectAPI => RF}
import etlflow.{EJPMType, JobEnv}
import zio.{Task, UIO, ZIO}

case class LocalExecutor(etl_job_name_package: String, config: Option[Config] = None, job_run_id: Option[String] = None, is_master: Option[String] = None) extends Service {
  override def executeJob(name: String, properties: Map[String, String]): ZIO[JobEnv, Throwable, Unit] = {
    val ejpm = RF.getEtlJobPropsMapping[EJPMType](name, etl_job_name_package)
    val job = ejpm.etlJob(properties)
    job.job_name = ejpm.toString
    job.job_enable_db_logging = ejpm.job_enable_db_logging
    job.job_send_slack_notification = ejpm.job_send_slack_notification
    job.job_notification_level = ejpm.job_notification_level
    JsonApi.convertToString[Map[String,String]](ejpm.getProps.mapValues(x => x.toString).toMap,List.empty).flatMap(props =>
      job.execute(config, job_run_id, is_master, props)
    )
  }
  private[etlflow] def showJobProps(name: String, properties: Map[String, String], etl_job_name_package: String): ZIO[JsonEnv, Throwable, Unit] = {
    val job_name = RF.getEtlJobPropsMapping[EJPMType](name,etl_job_name_package)
    val exclude_keys = List("job_run_id","job_description","job_properties")
    for{
      job_props <- Task(job_name.getProps.mapValues(x => x.toString) -- exclude_keys )
      _          = UIO(println(job_props))
    } yield ()
  }
  private[etlflow] def showJobStepProps(name: String, properties: Map[String, String], etl_job_name_package: String): ZIO[JsonEnv, Throwable, Unit] = {
    val job_name = RF.getEtlJobPropsMapping[EJPMType](name,etl_job_name_package)
    val etl_job = job_name.etlJob(properties)
    if (etl_job.isInstanceOf[SequentialEtlJob[_]]) {
      etl_job.job_name = job_name.toString
      JsonApi.convertToString(etl_job.getJobInfo(job_name.job_notification_level), List.empty).map(println(_))
    }
    else {
      UIO(println("Step Props info not available for generic jobs"))
    }
  }
}
