package examples.schema

import etlflow.etljobs.EtlJob
import etlflow.{EtlJobProps, EtlJobPropsMapping}
import examples.jobs._
import examples.schema.MyEtlJobProps._
import io.circe.generic.auto._

sealed trait MyEtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]] extends EtlJobPropsMapping[EJP,EJ]

object MyEtlJobPropsMapping {
  case object Job1 extends MyEtlJobPropsMapping[EtlJob1Props,Job1] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props(
      arg = job_properties.getOrElse("arg1","hello")
    )
  }
  case object Job2 extends MyEtlJobPropsMapping[EtlJob1Props,Job2] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props(
      arg = job_properties("arg1")
    )
  }
  case object Job3 extends MyEtlJobPropsMapping[EtlJob1Props,Job3] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props(
      arg = job_properties("arg1")
    )
  }
}
