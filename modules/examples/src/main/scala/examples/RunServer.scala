package examples

import etlflow.etljobs.EtlJob
import etlflow.scheduler.SchedulerApp
import examples.jobs._
import examples.schema.MyEtlJobName._
import examples.schema.{MyEtlJobName, MyEtlJobProps}
import scala.util.Try

object RunServer extends SchedulerApp[MyEtlJobName[MyEtlJobProps], MyEtlJobProps, MyGlobalProperties] {
  def globalProperties: Option[MyGlobalProperties] = Try(new MyGlobalProperties(sys.env.getOrElse("PROPERTIES_FILE_PATH","loaddata.properties"))).toOption
  override val etl_job_name_package: String = my_job_package

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
