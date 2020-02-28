package etljobs

import etljobs.etlsteps.StateLessEtlStep
import etljobs.log.{DbManager, SlackManager}
import etljobs.utils.GlobalProperties
import org.apache.log4j.Logger
import scala.util.{Failure, Success, Try}

trait EtlJob {
    val etl_job_logger: Logger = Logger.getLogger(getClass.getName)
    var job_execution_state: Map[String, Map[String,String]] = Map.empty
    var error_occurred: Boolean = false
    var aggregate_error: Boolean = false

    val etl_step_list: List[StateLessEtlStep]
    val job_name: EtlJobName
    val job_properties: EtlProps
    val global_properties: Option[GlobalProperties]

    def printJobInfo(level: String = "info"): Unit = {
      etl_step_list.foreach{ etl =>
        etl.getStepProperties(level).foreach(println)
      }
    }
    def getJobInfo(level: String = "info"): List[Map[String,String]] = {
      etl_step_list.map{ etl =>
        etl.getStepProperties(level)
      }
    }
    def execute(send_notification: Boolean = false, notification_level: String) : Map[String, Map[String,String]] = {
      val job_start_time = System.nanoTime()
      val job_run_id: String = job_properties.job_run_id
      aggregate_error = job_properties.aggregate_error

      if (send_notification)
      {
        SlackManager.final_message = ""
        SlackManager.job_name = job_name
        SlackManager.webhook_url = global_properties match {
          case Some(x) => x.slack_webhook_url
          case None => "<use_global_properties_slack_webhook_url>"
        }
        SlackManager.env = global_properties match {
          case Some(x) => x.slack_env
          case None => "<use_global_properties_slack_env>"
        }

        DbManager.job_run_id  = job_run_id
        DbManager.log_db_url  = global_properties match {
          case Some(x) => x.log_db_url
          case None => "<use_global_properties_log_db_url>"
        }
        DbManager.log_db_user = global_properties match {
          case Some(x) => x.log_db_user
          case None => "<use_global_properties_log_db_user>"
        }
        DbManager.log_db_pwd  = global_properties match {
          case Some(x) => x.log_db_pwd
          case None => "<use_global_properties_log_db_pwd>"
        }

        // Catch job result(Success/Failure) in Try so that it can be used further
        val job_result = Try{
          etl_step_list.foreach { etl =>
            val step_start_time = System.nanoTime()
            etl.process() match {
              case Success(_) =>
                DbManager.updateStepLevelInformation(step_start_time, etl, "Pass", notification_level)
                SlackManager.updateStepLevelInformation(step_start_time, etl, "Pass", notification_level)
                job_execution_state ++= etl.getExecutionMetrics
              case Failure(exception) =>
                SlackManager.updateStepLevelInformation(step_start_time, etl, "Failed", notification_level,Some(exception.getMessage))
                DbManager.updateStepLevelInformation(step_start_time, etl, exception.getMessage, notification_level,Some(exception.getMessage))
                job_execution_state ++= etl.getExecutionMetrics
                etl_job_logger.error("Error Occurred: " + exception.getMessage)
                if (aggregate_error)
                  error_occurred = true
                else
                  throw exception
            }
          }
        }

        job_result match {
          case Success(_) => if(error_occurred) {
                                SlackManager.sendNotification("Failed", job_start_time)
                                throw EtlJobException("Job failed")
                              }
                              else SlackManager.sendNotification("Pass", job_start_time)
          case Failure(e) => SlackManager.sendNotification("Failed", job_start_time); throw e;
        }
      }
      else
      {
        // Catch job result(Success/Failure) in Try so that it can be used further
        val job_result = Try{
          etl_step_list.foreach { etl =>
            val step_start_time = System.nanoTime()
            etl.process() match {
              case Success(_) => job_execution_state ++= etl.getExecutionMetrics
              case Failure(exception) =>
                job_execution_state ++= etl.getExecutionMetrics
                etl_job_logger.error("Error Occurred: " + exception.getMessage)

                if (aggregate_error)
                  error_occurred = true
                else
                  throw exception
            }
          }
        }

        job_result match {
          case Success(_) => if(error_occurred) { throw EtlJobException("Job failed"); }
          case Failure(e) => throw e;
        }
      }
      val job_end_time = System.nanoTime()
      etl_job_logger.info("Job completed successfully in : " + (job_end_time - job_start_time) / 1000000000.0 / 60.0 + " mins")
      job_execution_state
    }
}
