package examples

import etlflow.scheduler.executor.DataprocExecutor
import examples.schema.{MyEtlJobName, MyEtlJobProps}
import scala.util.Try

object RunDpServer extends DataprocExecutor[MyEtlJobName[MyEtlJobProps], MyEtlJobProps, MyGlobalProperties] {

  override def globalProperties: Option[MyGlobalProperties] = Try(new MyGlobalProperties(sys.env.getOrElse("PROPERTIES_FILE_PATH","loaddata.properties"))).toOption

  override val main_class: String = "examples.LoadData"
  override val dp_libs: List[String] = List.empty

}
