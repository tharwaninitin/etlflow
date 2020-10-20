package etlflow

import Schema._
import etlflow.etljobs.EtlJob
import etlflow.jobs.{EtlJob2DefinitionLocal, HelloWorldJob, Job3HttpSmtpSteps, Job4DBSteps}
import etlflow.utils.Executor.LOCAL_SUBPROCESS

sealed trait MyEtlJobName[+EJP <: EtlJobProps] extends EtlJobName[EJP]

object MyEtlJobName {
  case object EtlJob3 extends MyEtlJobName[EtlJob3Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob3Props = EtlJob3Props()
    def etlJob(job_properties: Map[String, String]): EtlJob[EtlJob3Props] = Job3HttpSmtpSteps(getActualProperties(job_properties))
  }
  case object EtlJob4 extends MyEtlJobName[EtlJob4Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props()
    def etlJob(job_properties: Map[String, String]): EtlJob[EtlJob4Props] = Job4DBSteps(getActualProperties(job_properties))
  }
  val local_subprocess = LOCAL_SUBPROCESS("examples/target/docker/stage/opt/docker/bin/load-data")
  case object EtlJob4LocalSubProcess extends MyEtlJobName[EtlJob4Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props(job_deploy_mode = local_subprocess)
    def etlJob(job_properties: Map[String, String]): EtlJob[EtlJob4Props] = HelloWorldJob(getActualProperties(job_properties))
  }

  case object Job2LocalJobGenericStep extends MyEtlJobName[LocalSampleProps] {
    override def getActualProperties(job_properties: Map[String, String]): LocalSampleProps = LocalSampleProps(job_deploy_mode = local_subprocess)
    def etlJob(job_properties: Map[String, String]): EtlJob[LocalSampleProps] = EtlJob2DefinitionLocal(getActualProperties(job_properties))

  }
}

