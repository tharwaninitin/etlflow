package etljobs

import com.google.cloud.bigquery.BigQuery
import etljobs.etlsteps.EtlStep
import etljobs.utils.SessionManager
import etljobs.utils.SlackManager
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import scala.util.{Failure, Success, Try}

case class EtlJobException(msg : String) extends Exception

trait EtlJob extends SessionManager {
  val etl_job_logger : Logger = Logger.getLogger(getClass.getName)
  var job_execution_state : Map[String, Map[String,String]] = Map.empty

  def apply() : List[EtlStep[Unit,Unit]]

  def printJobInfo() : Unit = {
    apply().foreach{ etl =>
      etl.getStepProperties.foreach(println)
    }
  }

  def getJobInfo() : List[Map[String,String]] = {
    apply().map{ etl =>
      etl.getStepProperties
    }
  }

  def execute(job_properties : Map[String,String] = Map()) : Map[String, Map[String,String]] = {
    
    val job_start_time = System.nanoTime()
    
    // Catch job result(Success/Failure) in Try so that it can be used further
    val job_result = Try{
      apply().foreach { etl =>
        val step_start_time = System.nanoTime()
        etl.process() match {
          case Success(_) => 
            SlackManager.updateStepLevelInformation(step_start_time, etl, "Pass")
            job_execution_state ++= etl.getExecutionMetrics
          case Failure(exception) => 
            SlackManager.updateStepLevelInformation(step_start_time, etl, "Failed", Some(exception.getMessage()));
            job_execution_state ++= etl.getExecutionMetrics 
            etl_job_logger.error("Error Occured: " + exception.getMessage())
            
            if (job_properties.get("aggregate_error").getOrElse("false") == "true")
              SlackManager.error_occured = true
            else
              throw exception   
        }
      }
    }
    
    val job_end_time = System.nanoTime()
    
    SlackManager.job_properties = job_properties
    SlackManager.webhook_url = settings.slack_webhook_url
    SlackManager.env = settings.slack_env

    job_result match {
      case Success(_) => if(SlackManager.error_occured) {
                            SlackManager.sendSlackNotification("Failed", job_start_time); 
                            throw EtlJobException("Job failed"); 
                          }
                          else SlackManager.sendSlackNotification("Pass", job_start_time)
      case Failure(e) => SlackManager.sendSlackNotification("Failed", job_start_time); throw e;
    }
    etl_job_logger.info("Job completed in : " + (job_end_time - job_start_time) / 1000000000.0 / 60.0 + " mins")
    job_execution_state
  }
}
