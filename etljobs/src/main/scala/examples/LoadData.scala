package examples

import etljobs.EtlJobApp
import etljobs.etljob.EtlJob
import examples.jobs._
import examples.schema.MyEtlJobName._
import examples.schema.{MyEtlJobName, MyEtlJobProps}
import scala.util.Try

object LoadData extends EtlJobApp[MyEtlJobName[MyEtlJobProps], MyEtlJobProps, MyGlobalProperties] {
  // Use AppLogger.initialize() to initialize logging
  // or keep log4j.properties in resources folder
  private val props_file_path = s"${new java.io.File(".").getCanonicalPath}/conf/loaddata.properties"
  override def globalProperties: Option[MyGlobalProperties] = Try(new MyGlobalProperties(props_file_path)).toOption
  override val etl_job_name_package = my_job_package
  override val region: String       = ""
  override val projectId: String    = ""
  override val endPoint: String     = ""
  override val clusterName: String  = ""
  override val mainClass: String    = ""
  override val libs: List[String]   = List.empty

  def toEtlJob(job_name: MyEtlJobName[MyEtlJobProps]): (MyEtlJobProps,Option[MyGlobalProperties]) => EtlJob = {
    job_name match {
      case EtlJob1PARQUETtoORCtoBQLocalWith2Steps => EtlJob1Definition
      case EtlJob2CSVtoPARQUETtoBQLocalWith3Steps => EtlJob2Definition
      case EtlJob3CSVtoCSVtoBQGcsWith2Steps       => EtlJob3Definition
      case EtlJob4BQtoBQ                          => EtlJob4Definition
      case EtlJob5PARQUETtoJDBC                   => EtlJob5Definition
    }
  }
}
