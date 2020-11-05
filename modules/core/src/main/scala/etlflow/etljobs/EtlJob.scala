package etlflow.etljobs

import etlflow.EtlJobProps
import etlflow.utils.{Configuration, LoggingLevel}
import org.slf4j.{Logger, LoggerFactory}
import zio._

trait EtlJob[+EJP <: EtlJobProps] extends Configuration {
  final val etl_job_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  var job_name: String = getClass.getName
  val job_properties: EJP
  val job_status: UIO[Ref[String]] = Ref.make("StatusNotSet")

  def printJobInfo(level: LoggingLevel = LoggingLevel.INFO): Unit
  def getJobInfo(level: LoggingLevel = LoggingLevel.INFO): List[(String,Map[String,String])]
  def execute(job_run_id:Option[String] = None): ZIO[ZEnv, Throwable, Unit]
}
