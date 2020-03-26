package etljobs.log

import etljobs.{EtlJobName, EtlJobProps}
import etljobs.etlsteps.EtlStep
import org.apache.log4j.Logger

trait LogManager {
  var job_properties: EtlJobProps
  val lm_logger: Logger = Logger.getLogger(getClass.getName)

  def updateStepLevelInformation(
                                  execution_start_time: Long,
                                  etl_step: EtlStep[Unit,Unit],
                                  state_status: String,
                                  error_message: Option[String] = None,
                                  mode: String = "update"
                                ): Unit
  def updateJobInformation(status: String, mode: String = "update"): Unit
}
