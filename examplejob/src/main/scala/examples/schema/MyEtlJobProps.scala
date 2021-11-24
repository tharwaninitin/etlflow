package examples.schema

import etlflow.EtlJobProps

sealed trait MyEtlJobProps extends EtlJobProps

object MyEtlJobProps {
  case class EtlJob1Props(arg: String) extends MyEtlJobProps
}
