package etlflow.coretests

import etlflow.coretests.Schema._
import etlflow.coretests.jobs._
import etlflow.etljobs.EtlJob
import etlflow.utils.Executor.LOCAL_SUBPROCESS
import etlflow.{EtlJobProps, EtlJobPropsMapping}

sealed trait MyEtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]] extends EtlJobPropsMapping[EJP,EJ]

object MyEtlJobPropsMapping {
  val local_subprocess: LOCAL_SUBPROCESS = LOCAL_SUBPROCESS("examples/target/docker/stage/opt/docker/bin/load-data")

  case object Job1 extends MyEtlJobPropsMapping[EtlJob1Props,Job1HelloWorld] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props()
  }

  case object Job2 extends MyEtlJobPropsMapping[EtlJob2Props,Job2Retry] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob2Props = EtlJob2Props()
  }

  case object Job3 extends MyEtlJobPropsMapping[EtlJob3Props,Job3HttpSmtpSteps] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob3Props = EtlJob3Props()
  }

  case object Job4 extends MyEtlJobPropsMapping[EtlJob4Props,Job4DBSteps] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props()
  }

  case object Job5 extends MyEtlJobPropsMapping[EtlJob5Props,Job5GenericSteps] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob5Props = EtlJob5Props()
  }
}

