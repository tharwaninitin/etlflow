package examples.schema

import etlflow.etljobs.EtlJob
import etlflow.{EtlJobProps, EtlJobPropsMapping}
import examples.jobs.JobDBSteps
import examples.schema.MyEtlJobProps._
import io.circe.generic.auto._

sealed trait MyEtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]] extends EtlJobPropsMapping[EJP,EJ]

object MyEtlJobPropsMapping {
  case object JobDBSteps extends MyEtlJobPropsMapping[EtlJob1Props,JobDBSteps] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props()
  }
}
