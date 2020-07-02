package etlflow.log

import etlflow.EtlJobProps
import etlflow.etlsteps.EtlStep
import org.slf4j.{Logger, LoggerFactory}

trait LogManager[A] {
  val job_name: String
  val job_properties: EtlJobProps
  val lm_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def updateStepLevelInformation(
                                  execution_start_time: Long,
                                  etl_step: EtlStep[_,_],
                                  state_status: String,
                                  error_message: Option[String] = None,
                                  mode: String = "update"
                                ): A
  def updateJobInformation(status: String, mode: String = "update", error_message: Option[String] = None): A
}
