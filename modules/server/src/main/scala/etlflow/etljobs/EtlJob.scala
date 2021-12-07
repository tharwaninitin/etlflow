package etlflow.etljobs

import etlflow.EtlJobProps
import etlflow.core.CoreLogEnv
import etlflow.utils.ApplicationLogger
import zio._

trait EtlJob[EJP <: EtlJobProps] extends ApplicationLogger {

  val job_properties: EJP
  var job_name: String = getClass.getName

  def execute(job_run_id: Option[String] = None, is_master: Option[String] = None, props: String = "{}"): RIO[CoreLogEnv, Unit]
}
