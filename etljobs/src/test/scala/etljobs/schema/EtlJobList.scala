package etljobs.schema

import etljobs.EtlJobName

object EtlJobList {
  sealed trait MyEtlJobName extends EtlJobName
  case object EtlJob1PARQUETtoORCtoBQLocalWith2StepsWithSlack extends MyEtlJobName
  case object EtlJob2CSVtoPARQUETtoBQLocalWith3Steps extends MyEtlJobName
  case object EtlJob3CSVtoPARQUETtoBQGcsWith2Steps extends MyEtlJobName
}
