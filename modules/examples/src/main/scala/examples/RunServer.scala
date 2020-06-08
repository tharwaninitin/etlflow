package examples

import etlflow.etljobs.EtlJob
import etlflow.scheduler.DataprocSchedulerApp
import examples.jobs._
import examples.schema.MyEtlJobName._
import examples.schema.{MyEtlJobName, MyEtlJobProps}
import scala.util.Try

object RunServer extends DataprocSchedulerApp[MyEtlJobName[MyEtlJobProps], MyEtlJobProps, MyGlobalProperties] {
  override def globalProperties: Option[MyGlobalProperties] = Try(new MyGlobalProperties(sys.env.getOrElse("PROPERTIES_FILE_PATH","loaddata.properties"))).toOption
  override val etl_job_name_package: String = my_job_package
  override val gcp_region: String = global_properties.map(x => x.gcp_region).getOrElse("<not_set>")
  override val gcp_project: String = global_properties.map(x => x.gcp_project).getOrElse("<not_set>")
  override val gcp_dp_endpoint: String = global_properties.map(x => x.gcp_dp_endpoint).getOrElse("<not_set>")
  override val gcp_dp_cluster_name: String = global_properties.map(x => x.gcp_dp_cluster_name).getOrElse("<not_set>")
  override val main_class: String = "examples.LoadData"
  override val dp_libs: List[String] = List.empty

  def toEtlJob(job_name: MyEtlJobName[MyEtlJobProps]): (MyEtlJobProps,Option[MyGlobalProperties]) => EtlJob = {
    job_name match {
      case EtlJob1PARQUETtoORCtoBQLocalWith2Steps => EtlJob1Definition
      case EtlJob2CSVtoPARQUETtoBQLocalWith3Steps => EtlJob2Definition
      case EtlJob3CSVtoCSVtoBQGcsWith2Steps => EtlJob3Definition
      case EtlJob4BQtoBQ => EtlJob4Definition
      case EtlJob5PARQUETtoJDBC => EtlJob5Definition
      case EtlJob6BQPGQuery => EtlJob6Definition
    }
  }
}
