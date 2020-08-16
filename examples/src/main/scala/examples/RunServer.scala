package examples

import etlflow.scheduler.Executor
import examples.schema.{MyEtlJobName, MyEtlJobProps}
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Level, Logger => LBLogger}
import etlflow.etljobs.EtlJob
import etlflow.utils.Config
import examples.jobs._
import examples.schema.MyEtlJobName._

object RunServer extends Executor[MyEtlJobName[MyEtlJobProps], MyEtlJobProps] {

  val spark_logger: LBLogger = LoggerFactory.getLogger("org.apache.spark").asInstanceOf[LBLogger]
  spark_logger.setLevel(Level.WARN)
  lazy val spark_jetty_logger: LBLogger = LoggerFactory.getLogger("org.spark_project.jetty").asInstanceOf[LBLogger]
  spark_jetty_logger.setLevel(Level.WARN)

  override val main_class: String = "examples.LoadData"
  override val dp_libs: List[String] = List.empty

  override def toEtlJob(job_name: MyEtlJobName[MyEtlJobProps]): (MyEtlJobProps, Config) => EtlJob = {
    job_name match {
      case Job0DataprocPARQUETtoORCtoBQ => EtlJob0DefinitionDataproc
      case Job1LocalJobDPSparkStep => EtlJob1DefinitionLocal
      case Job2LocalJobGenericStep => EtlJob2DefinitionLocal
      case EtlJob2CSVtoPARQUETtoBQLocalWith3Steps => EtlJob2Definition
      case EtlJob3CSVtoCSVtoBQGcsWith2Steps => EtlJob3Definition
      case EtlJob4BQtoBQ => EtlJob4Definition
      case EtlJob5PARQUETtoJDBC => EtlJob5Definition
      case EtlJob6BQPGQuery => EtlJob6Definition
    }
  }

}
