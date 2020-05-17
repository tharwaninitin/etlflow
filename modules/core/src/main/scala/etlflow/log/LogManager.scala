package etlflow.log

import etlflow.{EtlJobName, EtlJobProps}
import etlflow.etlsteps.EtlStep
import org.apache.log4j.Logger

trait LogManager[A] {
  val job_name: String
  val job_properties: EtlJobProps
  val lm_logger: Logger = Logger.getLogger(getClass.getName)

  def updateStepLevelInformation(
                                  execution_start_time: Long,
                                  etl_step: EtlStep[_,_],
                                  state_status: String,
                                  error_message: Option[String] = None,
                                  mode: String = "update"
                                ): A
  def updateJobInformation(status: String, mode: String = "update"): A
}
