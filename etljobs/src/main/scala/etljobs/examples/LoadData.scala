package etljobs.examples

import etljobs.{EtlJob, EtlJobApp}
import etljobs.examples.schema.{MyEtlJobName, MyEtlJobProps}
import etljobs.examples.schema.MyEtlJobName._
import scala.util.Try

object LoadData extends EtlJobApp[MyEtlJobName[MyEtlJobProps], MyEtlJobProps] {
  // Use AppLogger.initialize() to initialize logging
  // or keep log4j.properties in resources folder
  private val props_file_path = s"${new java.io.File(".").getCanonicalPath}/conf/loaddata.properties"
  lazy val global_properties: Option[MyGlobalProperties] = Try(new MyGlobalProperties(props_file_path)).toOption
  val etl_job_list_package: String = "etljobs.examples.schema.MyEtlJobName$"

  def toEtlJob(job_name: MyEtlJobName[MyEtlJobProps], job_properties: Map[String, String]): EtlJob = {
    job_name match {
      case x@EtlJob1PARQUETtoORCtoBQLocalWith2Steps => etljob1.EtlJobDefinition(x.toString, x.getActualProperties(job_properties), global_properties)
      case x@EtlJob2CSVtoPARQUETtoBQLocalWith3Steps => etljob2.EtlJobDefinition(x.toString, x.getActualProperties(job_properties), global_properties)
      case x@EtlJob3CSVtoCSVtoBQGcsWith2Steps => new etljob3.EtlJobDefinition(x.toString, x.getActualProperties(job_properties), global_properties)
      case x@EtlJob4BQtoBQ => etljob4.EtlJobDefinition(x.toString, x.getActualProperties(job_properties))
      case x@EtlJob5PARQUETtoJDBC => etljob5.EtlJobDefinition(x.toString, x.getActualProperties(job_properties), global_properties)
    }
  }
}
