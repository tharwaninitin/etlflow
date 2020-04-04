package etljobs.schema

import etljobs.{EtlJobName}
import etljobs.schema.MyEtlJobProps._

object MyEtlJobList {
  sealed trait MyEtlJobName[+EJP] extends EtlJobName[EJP]
  case object EtlJob1PARQUETtoORCtoBQLocalWith2Steps extends MyEtlJobName[EtlJob1Props] {
    val job_props: etljobs.EtlJobProps = EtlJob1Props()
  }
  case object EtlJob2CSVtoPARQUETtoBQLocalWith3Steps extends MyEtlJobName[EtlJob23Props] {
    val job_props: etljobs.EtlJobProps = EtlJob23Props()
  }
  case object EtlJob3CSVtoCSVtoBQGcsWith2Steps extends MyEtlJobName[EtlJob23Props] {
    val job_props: etljobs.EtlJobProps = EtlJob23Props()
  }
  case object EtlJob4BQtoBQ extends MyEtlJobName[EtlJob4Props] {
    val job_props: etljobs.EtlJobProps = EtlJob4Props()
  }
  case object EtlJob5PARQUETtoJDBC extends MyEtlJobName[EtlJob5Props] {
    val job_props: etljobs.EtlJobProps = EtlJob4Props()
  }

  val catOneJobList   = List(EtlJob1PARQUETtoORCtoBQLocalWith2Steps)
  val catTwoJobList   = List(EtlJob2CSVtoPARQUETtoBQLocalWith3Steps,EtlJob3CSVtoCSVtoBQGcsWith2Steps)
  val catThreeJobList = List(EtlJob4BQtoBQ)
  val catFourJobList  = List(EtlJob5PARQUETtoJDBC)
}
