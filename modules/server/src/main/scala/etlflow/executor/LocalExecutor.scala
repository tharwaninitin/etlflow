package etlflow.executor

import etlflow.core.CoreEnv
import etlflow.json.JsonEnv
import etlflow.schema.Config
import etlflow.utils.{ApplicationLogger, ReflectAPI => RF}
import etlflow.{EJPMType, json, log}
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

  def executeJob(name: String, properties: Map[String, String], config: Config, job_run_id: String): RIO[CoreEnv with JsonEnv, Unit] = {
    for {
      ejpm  <- RF.getJob[T](name)
      job   = ejpm.etlJob(properties)
      _     = { job.job_name = ejpm.toString }
      args  <- ejpm.getActualPropertiesAsJson(properties)
      dblog = if(config.db.isEmpty) log.nolog else log.DB(config.db.get, job_run_id, "Job-" + name + "-Pool")
      _     <- job.execute(args).provideSomeLayer[CoreEnv with JsonEnv](dblog)
    } yield ()
  }

  def runJob(name: String, properties: Map[String,String], config: Config): RIO[CoreEnv, Unit] = {
    val jri         = if (properties.keySet.contains("job_run_id")) properties("job_run_id") else java.util.UUID.randomUUID.toString
    val jsonLayer   = json.Implementation.live
    executeJob(name, properties, config, jri).provideSomeLayer[CoreEnv](jsonLayer)
  }
}
