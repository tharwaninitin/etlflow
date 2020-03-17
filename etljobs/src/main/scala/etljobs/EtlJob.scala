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
    def execute(send_slack_notification: Boolean = false, log_in_db: Boolean = false, notification_level: String) : Map[String, Map[String,String]] = {
      val job_start_time = System.currentTimeMillis()
      val job_run_id: String = job_properties.job_run_id
      aggregate_error = job_properties.aggregate_error

      if (send_slack_notification || log_in_db)
      {
        SlackManager.final_message = ""
        SlackManager.job_name      = job_name
        SlackManager.webhook_url   = global_properties match {
          case Some(x) => x.slack_webhook_url
          case None => "<use_global_properties_slack_webhook_url>"
        }
        SlackManager.env           = global_properties match {
          case Some(x) => x.slack_env
          case None => "<use_global_properties_slack_env>"
        }
        SlackManager.log_level     = notification_level

        DbManager.job_run_id  = job_run_id
        DbManager.job_name    = job_name
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
        DbManager.log_level   = notification_level

        if (log_in_db) DbManager.updateJobInformation("started", Some(job_properties),"insert")
        // Catch job result(Success/Failure) in Try so that it can be used further
        val job_result = Try{
          etl_step_list.foreach { etl =>
            val step_start_time = System.currentTimeMillis()
            if (log_in_db) DbManager.updateStepLevelInformation(step_start_time, etl, "started", notification_level, mode = "insert")
            etl.process() match {
              case Success(_) =>
                if (send_slack_notification) SlackManager.updateStepLevelInformation(step_start_time, etl, "pass", notification_level)
                if (log_in_db) DbManager.updateStepLevelInformation(step_start_time, etl, "pass", notification_level)
                job_execution_state ++= etl.getExecutionMetrics
              case Failure(exception) =>
                if (send_slack_notification) SlackManager.updateStepLevelInformation(step_start_time, etl, "failed", notification_level, Some(exception.getMessage))
                if (log_in_db) DbManager.updateStepLevelInformation(step_start_time, etl, "failed", notification_level, Some(exception.getMessage))
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
                                if (send_slack_notification) SlackManager.sendNotification("failed", job_start_time)
                                if (log_in_db) DbManager.updateJobInformation("failed")
                                throw EtlJobException("Job failed")
                              }
                              else {
                                if (send_slack_notification) SlackManager.sendNotification("pass", job_start_time)
                                if (log_in_db) DbManager.updateJobInformation("pass")
                              }
          case Failure(e) => if (send_slack_notification) SlackManager.sendNotification("failed", job_start_time)
                             if (log_in_db) DbManager.updateJobInformation("failed")
                             throw e
        }
      }
      else
      {
        // Catch job result(Success/Failure) in Try so that it can be used further
        val job_result = Try{
          etl_step_list.foreach { etl =>
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
      val job_end_time = System.currentTimeMillis()
      etl_job_logger.info("Job completed successfully in : " + (job_end_time - job_start_time) / 1000.0 / 60.0 + " mins")
      job_execution_state
    }
}
