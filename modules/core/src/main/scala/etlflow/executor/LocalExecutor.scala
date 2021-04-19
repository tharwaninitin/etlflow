package etlflow.executor

import etlflow.{EJPMType, JobEnv}
import etlflow.etljobs.SequentialEtlJob
import etlflow.utils.{JsonJackson, UtilityFunctions => UF}
import zio.{Task, UIO, ZIO}

case class LocalExecutor(etl_job_name_package: String, job_run_id: Option[String] = None, is_master: Option[String] = None) extends Service {
  override def executeJob(name: String, properties: Map[String, String]): ZIO[JobEnv, Throwable, Unit] = {
    val job_name = UF.getEtlJobName[EJPMType](name, etl_job_name_package)
    val job = job_name.etlJob(properties)
    job.job_name = job_name.toString
    job.job_enable_db_logging = job_name.job_enable_db_logging
    job.job_send_slack_notification = job_name.job_send_slack_notification
    job.job_notification_level = job_name.job_notification_level
    job.execute(job_run_id, is_master)
  }
  def showJobProps(name: String, properties: Map[String, String], etl_job_name_package: String): Task[Unit] = {
    val job_name = UF.getEtlJobName[EJPMType](name,etl_job_name_package)
    val exclude_keys = List("job_run_id","job_description","job_properties")
    val props = job_name.getActualProperties(properties)
    UIO(println(JsonJackson.convertToJsonByRemovingKeys(props,exclude_keys)))
  }
  def showJobStepProps(name: String, properties: Map[String, String], etl_job_name_package: String): Task[Unit] = {
    val job_name = UF.getEtlJobName[EJPMType](name,etl_job_name_package)
    val etl_job = job_name.etlJob(properties)
    if (etl_job.isInstanceOf[SequentialEtlJob[_]]) {
      etl_job.job_name = job_name.toString
      val json = JsonJackson.convertToJson(etl_job.getJobInfo(job_name.job_notification_level))
      UIO(println(json))
    }
    else {
      UIO(println("Step Props info not available for generic jobs"))
    }
  }
}
