package etlflow

import etlflow.etljobs.EtlJob
import etlflow.jobs._
import etlflow.MyEtlJobName._
import etlflow.utils.Config

object LoadData extends EtlJobApp[MyEtlJobName[EtlJobProps], EtlJobProps] with TestSuiteHelper {

  override def toEtlJob(job_name: MyEtlJobName[EtlJobProps]): (EtlJobProps, Config) => EtlJob = {
    job_name match {
      case EtlJob3 => Job3HttpSmtpSteps
      case EtlJob4 => Job4DBSteps
    }
  }
}
