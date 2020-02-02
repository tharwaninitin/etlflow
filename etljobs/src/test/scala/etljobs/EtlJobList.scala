package etljobs

object EtlJobList {
  case object EtlJob1PARQUETtoORCtoBQLocalWith2StepsWithSlack extends EtlJobName
  case object EtlJob2CSVtoPARQUETtoBQLocalWith3Steps extends EtlJobName
  case object EtlJob3CSVtoPARQUETtoBQGcsWith2Steps extends EtlJobName
}
