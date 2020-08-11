package etlflow

import etlflow.etljobs.EtlJob
import etlflow.jobs._
import etlflow.MyEtlJobName._
import etlflow.utils.{Config, GlobalProperties}

object LoadData extends EtlJobApp[MyEtlJobName[EtlJobProps], EtlJobProps] with TestSuiteHelper {

  override def toEtlJob(job_name: MyEtlJobName[EtlJobProps]): (EtlJobProps, Config) => EtlJob = {
    job_name match {
      case EtlJob1 => EtlJob1Definition
      case EtlJob2 => EtlJob2Definition
      case EtlJob3 => EtlJob3Definition
      case EtlJob4 => EtlJob4Definition
    }
  }
}
