package etlflow.scheduler.schema

import etlflow.EtlJobProps
import etlflow.utils.Executor
import etlflow.utils.Executor.LOCAL_SUBPROCESS

sealed trait MyEtlJobProps extends EtlJobProps

object MyEtlJobProps {

  val local_subprocess = LOCAL_SUBPROCESS("target/universal/stage/bin/load-data")
  case class EtlJob4Props(override val job_deploy_mode: Executor = local_subprocess) extends MyEtlJobProps

}
