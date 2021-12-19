package etlflow.executor

import etlflow.json.JsonEnv
import etlflow.schema.Config
import etlflow.utils.{ApplicationLogger, ReflectAPI => RF}
import etlflow.{EJPMType, JobEnv, crypto, json, log}
import zio._

case class LocalExecutor[T <: EJPMType : Tag]() extends ApplicationLogger {

  def listJobs: Task[Unit] = RF.getSubClasses[T].map(_.foreach(logger.info))

  def showJobProps(name: String): Task[Unit] = {
    val exclude_keys = List("job_run_id","job_description","job_properties")
    for {
      ejpm      <- RF.getJob[T](name)
      job_props = ejpm.getProps -- exclude_keys
      _         = println(job_props)
    } yield ()
  }

  def getActualJobProps(name: String, properties: Map[String, String]): RIO[JsonEnv, String] = {
    for {
      ejpm      <- RF.getJob[T](name)
      job_props <- ejpm.getActualPropertiesAsJson(properties)
    } yield job_props
  }

  def executeJob(name: String, properties: Map[String, String], job_run_id: Option[String] = None): RIO[JobEnv, Unit] = {
    for {
      ejpm <- RF.getJob[T](name)
      job  = ejpm.etlJob(properties)
      _    = { job.job_name = ejpm.toString }
      args <- ejpm.getActualPropertiesAsJson(properties)
      _     <- job.execute(job_run_id, args)
    } yield ()
  }

  def runJob(name: String, properties: Map[String,String], config: Config): RIO[ZEnv, Unit] = {
    val jri         = if (properties.keySet.contains("job_run_id")) Some(properties("job_run_id")) else None
    val dbLogLayer  = if(config.db.isEmpty) log.nolog else log.DB(config.db.get, "Job-" + name + "-Pool")
    val jsonLayer   = json.Implementation.live
    val cryptoLayer = crypto.Implementation.live(config.secretkey)
    executeJob(name, properties, jri)
      .provideCustomLayer(dbLogLayer ++ jsonLayer ++ cryptoLayer)
  }
}
