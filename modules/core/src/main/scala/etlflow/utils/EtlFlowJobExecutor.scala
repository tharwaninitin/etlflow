package etlflow.utils

import etlflow.executor.LocalExecutor
import etlflow.schema.Config
import etlflow.{EJPMType, crypto, db, json, log}
import etlflow.utils.{ReflectAPI => RF}
import zio.{Tag, ZIO}

class EtlFlowJobExecutor[T <: EJPMType : Tag] extends ApplicationLogger {

  val localExecutor = LocalExecutor[T]()

  def list_jobs: ZIO[Any, Throwable, Unit] = {
    RF.getSubClasses[T].map(_.foreach(logger.info))
  }

  def show_job_props(job_name:String): ZIO[zio.ZEnv, Throwable, Unit] = {
    localExecutor
      .showJobProps(job_name)
      .provideCustomLayer(json.Implementation.live)
  }

  def show_step_props(job_name:String, job_properties: Map[String,String]): ZIO[zio.ZEnv, Throwable, Unit] = {
    localExecutor
      .showJobStepProps(job_name, job_properties)
      .provideCustomLayer(json.Implementation.live)
  }

  def run_job(job_name: String, job_properties: Map[String,String], config: Config): ZIO[zio.ZEnv, Throwable, Unit] = {
    val jri         = if (job_properties.keySet.contains("job_run_id")) Some(job_properties("job_run_id")) else None
    val is_master   = if (job_properties.keySet.contains("is_master")) Some(job_properties("is_master")) else None
    val dbLayer     = if(config.db.isEmpty) db.noLog else db.liveDB(config.db.get, "Job-" + job_name + "-Pool", 2)
    val jsonLayer   = json.Implementation.live
    val cryptoLayer = crypto.Implementation.live(config.secretkey)
    val logLayer    = log.Implementation.live
    localExecutor
      .executeJob(job_name, job_properties, config.slack, jri, is_master)
      .provideCustomLayer(dbLayer ++ jsonLayer ++ cryptoLayer ++ logLayer)
  }
}
