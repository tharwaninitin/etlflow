package etlflow.scheduler

import etlflow.Schema.EtlJob4Props
import etlflow.etljobs.EtlJob
import etlflow.jobs.HelloWorldJob
import etlflow.utils.Executor.LOCAL_SUBPROCESS
import etlflow.{EtlJobName, EtlJobProps}

sealed trait MyEtlJobName[+EJP <: EtlJobProps] extends EtlJobName[EJP]

object MyEtlJobName {
  val local_subprocess = LOCAL_SUBPROCESS("target/universal/stage/bin/load-data")
  case object EtlJob4 extends MyEtlJobName[EtlJob4Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props(job_deploy_mode = local_subprocess)
    def etlJob(job_properties: Map[String, String]): EtlJob[EtlJob4Props] = HelloWorldJob(getActualProperties(job_properties))
  }
}
