package etljobs

import etljobs.etlsteps.StateLessEtlStep
import etljobs.log.{DbManager, SlackManager}
import etljobs.utils.{GlobalProperties, UtilityFunctions => UF}
import org.apache.log4j.Logger
import scala.util.{Failure, Success, Try}

trait EtlJob {
    final val etl_job_logger: Logger = Logger.getLogger(getClass.getName)
    final var job_execution_state: Map[String, Map[String,String]] = Map.empty
    final var error_occurred: Boolean = false
    final var job_name: String = "NameNotSet"

    val job_properties: EtlJobProps
    val global_properties: Option[GlobalProperties]
    val etl_step_list: List[StateLessEtlStep]

    def printJobInfo(level: String = "info"): Unit = {
      etl_step_list.foreach{ etl =>
        etl.getStepProperties(level).foreach(println)
      }
    }
    def getJobInfo(level: String = "info"): List[(String,Map[String,String])] = {
      etl_step_list.flatMap{ etl_step =>
        Map(etl_step.name -> etl_step.getStepProperties(level))
      }
    }
    def execute: Map[String, Map[String,String]] = {
      val job_start_time = UF.getCurrentTimestamp

      if (job_properties.job_send_slack_notification) {
        SlackManager.job_name = job_name
        SlackManager.job_properties = job_properties
        SlackManager.final_message = ""
        SlackManager.web_hook_url = global_properties match {
          case Some(x) => x.slack_webhook_url
          case None => "<use_global_properties_slack_webhook_url>"
        }
        SlackManager.env = global_properties match {
          case Some(x) => x.slack_env
          case None => "<use_global_properties_slack_env>"
        }
      }

      DbManager.job_name = job_name
      DbManager.job_properties = job_properties
      DbManager.log_db_url = global_properties match {
        case Some(x) => x.log_db_url
        case None => "<use_global_properties_log_db_url>"
      }
      DbManager.log_db_user = global_properties match {
        case Some(x) => x.log_db_user
        case None => "<use_global_properties_log_db_user>"
      }
      DbManager.log_db_pwd = global_properties match {
        case Some(x) => x.log_db_pwd
        case None => "<use_global_properties_log_db_pwd>"
      }

      DbManager.updateJobInformation("started","insert")
      // Catch job result(Success/Failure) in Try so that it can be used further
      val job_result = Try{
        etl_step_list.foreach { etl =>
          val step_start_time = System.currentTimeMillis()
          DbManager.updateStepLevelInformation(step_start_time, etl, "started", mode = "insert")
          etl.process() match {
            case Success(_) =>
              if (job_properties.job_send_slack_notification) SlackManager.updateStepLevelInformation(step_start_time, etl, "pass")
              DbManager.updateStepLevelInformation(step_start_time, etl, "pass")
              job_execution_state ++= etl.getExecutionMetrics
            case Failure(exception) =>
              if (job_properties.job_send_slack_notification) SlackManager.updateStepLevelInformation(step_start_time, etl, "failed", Some(exception.getMessage))
              DbManager.updateStepLevelInformation(step_start_time, etl, "failed", Some(exception.getMessage))
              job_execution_state ++= etl.getExecutionMetrics
              etl_job_logger.error("Error Occurred, " + exception.getMessage)
              if (job_properties.job_aggregate_error)
                error_occurred = true
              else
                throw exception
          }
        }
      }

      job_result match {
        case Success(_) => if(error_occurred) {
                              if (job_properties.job_send_slack_notification) SlackManager.updateJobInformation("failed")
                              DbManager.updateJobInformation("failed")
                              etl_job_logger.info(s"Job ran in: ${UF.getTimeDifferenceAsString(job_start_time, UF.getCurrentTimestamp)}")
                              throw EtlJobException("Job failed")
                            }
                            else {
                              if (job_properties.job_send_slack_notification) SlackManager.updateJobInformation("pass")
                              DbManager.updateJobInformation("pass")
                              etl_job_logger.info(s"Job completed successfully in ${UF.getTimeDifferenceAsString(job_start_time, UF.getCurrentTimestamp)}")
                              job_execution_state
                            }
        case Failure(e) => if (job_properties.job_send_slack_notification) SlackManager.updateJobInformation("failed")
                           DbManager.updateJobInformation("failed")
                           etl_job_logger.info(s"Job completed in ${UF.getTimeDifferenceAsString(job_start_time, UF.getCurrentTimestamp)}")
                           throw e
      }

    }
}
