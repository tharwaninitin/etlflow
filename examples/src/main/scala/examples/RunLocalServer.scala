package examples

import etlflow.etljobs.EtlJob
import etlflow.scheduler.executor.LocalExecutor
import examples.jobs._
import examples.schema.MyEtlJobName._
import examples.schema.{MyEtlJobName, MyEtlJobProps}
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Level, Logger => LBLogger}
import etlflow.utils.Config

object RunLocalServer extends LocalExecutor[MyEtlJobName[MyEtlJobProps], MyEtlJobProps] {

  val spark_logger: LBLogger = LoggerFactory.getLogger("org.apache.spark").asInstanceOf[LBLogger]
  spark_logger.setLevel(Level.WARN)
  lazy val spark_jetty_logger: LBLogger = LoggerFactory.getLogger("org.spark_project.jetty").asInstanceOf[LBLogger]
  spark_jetty_logger.setLevel(Level.WARN)

  override def toEtlJob(job_name: MyEtlJobName[MyEtlJobProps]): (MyEtlJobProps, Config) => EtlJob = {
    job_name match {
      case EtlJob1DPTrigger => EtlJob1Trigger
      case EtlJob1PARQUETtoORCtoBQLocalWith2Steps => EtlJob1Definition
      case EtlJob2CSVtoPARQUETtoBQLocalWith3Steps => EtlJob2Definition
      case EtlJob3CSVtoCSVtoBQGcsWith2Steps => EtlJob3Definition
      case EtlJob4BQtoBQ => EtlJob4Definition
      case EtlJob5PARQUETtoJDBC => EtlJob5Definition
      case EtlJob6BQPGQuery => EtlJob6Definition
    }
  }
}
