package etljobs.schema

import etljobs.EtlJobName

object EtlJobList {
  sealed trait MyEtlJobName extends EtlJobName
  case object EtlJob1PARQUETtoORCtoBQLocalWith2Steps extends MyEtlJobName
  case object EtlJob2CSVtoPARQUETtoBQLocalWith3Steps extends MyEtlJobName
  case object EtlJob3CSVtoCSVtoBQGcsWith2Steps extends MyEtlJobName
  case object EtlJob4BQtoBQ extends MyEtlJobName
  case object EtlJob5PARQUETtoJDBC extends MyEtlJobName

  val catOneJobList   = List(EtlJob1PARQUETtoORCtoBQLocalWith2Steps)
  val catTwoJobList   = List(EtlJob2CSVtoPARQUETtoBQLocalWith3Steps,EtlJob3CSVtoCSVtoBQGcsWith2Steps)
  val catThreeJobList = List(EtlJob4BQtoBQ)
  val catFourJobList  = List(EtlJob5PARQUETtoJDBC)
}
