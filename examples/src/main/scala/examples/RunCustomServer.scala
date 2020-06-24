package examples

import etlflow.scheduler.CustomSchedulerApp
import examples.schema.{MyEtlJobName, MyEtlJobProps}
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Level, Logger => LBLogger}
import etlflow.etljobs.EtlJob
import examples.jobs._
import examples.schema.MyEtlJobName._
import scala.util.Try

object RunCustomServer extends CustomSchedulerApp[MyEtlJobName[MyEtlJobProps], MyEtlJobProps, MyGlobalProperties] {

  val spark_logger: LBLogger = LoggerFactory.getLogger("org.apache.spark").asInstanceOf[LBLogger]
  spark_logger.setLevel(Level.WARN)
  lazy val spark_jetty_logger: LBLogger = LoggerFactory.getLogger("org.spark_project.jetty").asInstanceOf[LBLogger]
  spark_jetty_logger.setLevel(Level.WARN)

  override def globalProperties: Option[MyGlobalProperties] = Try(new MyGlobalProperties(sys.env.getOrElse("PROPERTIES_FILE_PATH","loaddata.properties"))).toOption

  override val gcp_region: String = global_properties.map(x => x.gcp_region).getOrElse("<not_set>")
  override val gcp_project: String = global_properties.map(x => x.gcp_project).getOrElse("<not_set>")
  override val gcp_dp_endpoint: String = global_properties.map(x => x.gcp_dp_endpoint).getOrElse("<not_set>")
  override val gcp_dp_cluster_name: String = global_properties.map(x => x.gcp_dp_cluster_name).getOrElse("<not_set>")
  override val main_class: String = "examples.LoadData"
  override val dp_libs: List[String] = List.empty

  override def toEtlJob(job_name: MyEtlJobName[MyEtlJobProps]): (MyEtlJobProps, Option[MyGlobalProperties]) => EtlJob = {
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
