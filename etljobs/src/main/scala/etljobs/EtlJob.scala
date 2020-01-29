package etljobs

import etljobs.etlsteps.EtlStep
import etljobs.utils.{SessionManager, SlackManager, GlobalProperties}
import org.apache.log4j.Logger
import scala.util.{Failure, Success, Try}

case class EtlJobException(msg : String) extends Exception

abstract class EtlJob(job_properties : Map[String,String], global_properties: Option[GlobalProperties] = None, etl_props: EtlProps = new EtlProps {}) extends SessionManager(global_properties) {
  val etl_job_logger : Logger = Logger.getLogger(getClass.getName)
  var job_execution_state : Map[String, Map[String,String]] = Map.empty
  var error_occured : Boolean = false
  var slack_notification_level : String = "info"
  def apply() : List[EtlStep[Unit,Unit]]

  def printJobInfo(): Unit = {
    apply().foreach{ etl =>
      etl.getStepProperties(slack_notification_level).foreach(println)
    }
  }

  def getJobInfo: List[Map[String,String]] = {
    apply().map{ etl =>
      etl.getStepProperties(slack_notification_level)
    }
  }

  def execute(send_slack_notification: Boolean = false, slack_notification_level:String) : Map[String, Map[String,String]] = {
    val job_start_time = System.nanoTime()
    if (send_slack_notification) 
    {
      SlackManager.final_slack_message = ""
      SlackManager.job_properties = job_properties
      SlackManager.webhook_url = global_properties match {
        case Some(x) => x.slack_webhook_url
        case None => "<use_global_properties>"
      }
      SlackManager.env = global_properties match {
        case Some(x) => x.slack_env
        case None => "<use_global_properties>"
      }

      // Catch job result(Success/Failure) in Try so that it can be used further
      val job_result = Try{
        apply().foreach { etl =>
          val step_start_time = System.nanoTime()
          etl.process() match {
            case Success(_) => 
              SlackManager.updateStepLevelInformation(step_start_time, etl, "Pass", slack_notification_level)
              job_execution_state ++= etl.getExecutionMetrics
            case Failure(exception) => 
              SlackManager.updateStepLevelInformation(step_start_time, etl, "Failed", slack_notification_level,Some(exception.getMessage()));
              job_execution_state ++= etl.getExecutionMetrics 
              etl_job_logger.error("Error Occured: " + exception.getMessage())
              
              if (job_properties.get("aggregate_error").getOrElse("false") == "true")
                error_occured = true
              else
                throw exception   
          }
        }
      }
      
      job_result match {
        case Success(_) => if(error_occured) {
                              SlackManager.sendSlackNotification("Failed", job_start_time); 
                              throw EtlJobException("Job failed"); 
                            }
                            else SlackManager.sendSlackNotification("Pass", job_start_time)
        case Failure(e) => SlackManager.sendSlackNotification("Failed", job_start_time); throw e;
      }
    }
    else 
    {
      // Catch job result(Success/Failure) in Try so that it can be used further
      val job_result = Try{
        apply().foreach { etl =>
          val step_start_time = System.nanoTime()
          etl.process() match {
            case Success(_) => job_execution_state ++= etl.getExecutionMetrics
            case Failure(exception) => 
              job_execution_state ++= etl.getExecutionMetrics 
              etl_job_logger.error("Error Occured: " + exception.getMessage())
              
              if (job_properties.get("aggregate_error").getOrElse("false") == "true")
                error_occured = true
              else
                throw exception  
          }
        }
      }
      
      job_result match {
        case Success(_) => if(error_occured) { throw EtlJobException("Job failed"); }
        case Failure(e) => throw e;
      }
    }
    val job_end_time = System.nanoTime()
    etl_job_logger.info("Job completed successfully in : " + (job_end_time - job_start_time) / 1000000000.0 / 60.0 + " mins")
    job_execution_state
  }
}
