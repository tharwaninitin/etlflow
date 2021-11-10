package examples.schema

import etlflow.etljobs.EtlJob
import etlflow.{EtlJobProps, EtlJobPropsMapping}
import examples.jobs._
import examples.schema.MyEtlJobProps._
import io.circe.generic.auto._

sealed trait MyEtlJobPropsMapping[EJP <: EtlJobProps, EJ <: EtlJob[EJP]] extends EtlJobPropsMapping[EJP,EJ]

object MyEtlJobPropsMapping {
  case object DBJob extends MyEtlJobPropsMapping[EtlJob1Props,DBJob] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props()
  }
  case object HelloWorldJob extends MyEtlJobPropsMapping[EtlJob1Props,HelloWorldJob] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob1Props = EtlJob1Props()
  }
}
