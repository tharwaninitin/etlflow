package examples.schema

import etlflow.EtlJobProps
import etlflow.schema.Executor.{DATAPROC, KUBERNETES, LOCAL_SUBPROCESS}

sealed trait MyEtlJobProps extends EtlJobProps

object MyEtlJobProps {
  case class EtlJob1Props() extends MyEtlJobProps
}
