package etlflow.scheduler.schema

import etlflow.EtlJobName
import etlflow.scheduler.schema.MyEtlJobProps.EtlJob4Props

sealed trait MyEtlJobName[+EJP] extends EtlJobName[EJP]

object MyEtlJobName {

  case object EtlJobBarcWeekMonthToDate extends MyEtlJobName[EtlJob4Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props()
  }

  case object EtlJob4BQtoBQ extends MyEtlJobName[EtlJob4Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props()
  }

  case object EtlJob4 extends MyEtlJobName[EtlJob4Props] {
    def getActualProperties(job_properties: Map[String, String]): EtlJob4Props = EtlJob4Props()
  }
}
