package etljobs.examples

import etljobs.{EtlJob, EtlJobApp}
import etljobs.examples.schema.{MyEtlJobName, MyEtlJobProps}
import etljobs.examples.schema.MyEtlJobName._
import scala.util.Try

object LoadData extends EtlJobApp[MyEtlJobName[MyEtlJobProps], MyEtlJobProps, MyGlobalProperties] {
  // Use AppLogger.initialize() to initialize logging
  // or keep log4j.properties in resources folder
  private val props_file_path = s"${new java.io.File(".").getCanonicalPath}/conf/loaddata.properties"
  val global_properties: Option[MyGlobalProperties] = Try(new MyGlobalProperties(props_file_path)).toOption
  val etl_job_name_package: String = "etljobs.examples.schema.MyEtlJobName$"

  def toEtlJob(job_name: MyEtlJobName[MyEtlJobProps], job_properties: Map[String, String]): (MyEtlJobProps,Option[MyGlobalProperties]) => EtlJob = {
    job_name match {
      case EtlJob1PARQUETtoORCtoBQLocalWith2Steps => etljob1.EtlJobDefinition
      case EtlJob2CSVtoPARQUETtoBQLocalWith3Steps => etljob2.EtlJobDefinition
      case EtlJob3CSVtoCSVtoBQGcsWith2Steps       => etljob3.EtlJobDefinition
      case EtlJob4BQtoBQ                          => etljob4.EtlJobDefinition
      case EtlJob5PARQUETtoJDBC                   => etljob5.EtlJobDefinition
    }
  }
}
