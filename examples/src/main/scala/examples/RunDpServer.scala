package examples

import etlflow.scheduler.executor.DataprocExecutor
import examples.schema.{MyEtlJobName, MyEtlJobProps}
import scala.util.Try

object RunDpServer extends DataprocExecutor[MyEtlJobName[MyEtlJobProps], MyEtlJobProps, MyGlobalProperties] {

  override def globalProperties: Option[MyGlobalProperties] = Try(new MyGlobalProperties(sys.env.getOrElse("PROPERTIES_FILE_PATH","loaddata.properties"))).toOption

  override val gcp_region: String = global_properties.map(x => x.gcp_region).getOrElse("<not_set>")
  override val gcp_project: String = global_properties.map(x => x.gcp_project).getOrElse("<not_set>")
  override val gcp_dp_endpoint: String = global_properties.map(x => x.gcp_dp_endpoint).getOrElse("<not_set>")
  override val gcp_dp_cluster_name: String = global_properties.map(x => x.gcp_dp_cluster_name).getOrElse("<not_set>")
  override val main_class: String = "examples.LoadData"
  override val dp_libs: List[String] = List.empty

}
