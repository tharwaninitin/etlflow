package etljobs.utils

import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

abstract class GlobalProperties(global_properties_file_path: String, job_properties: Map[String,String] = Map.empty) {
  private val ic_logger = Logger.getLogger(getClass.getName)
  ic_logger.info(f"======> Trying to load ${getClass.getName} with path $global_properties_file_path")

  val configTry: Try[Properties] = Try {
    val file_stream = new FileInputStream(global_properties_file_path)
    val prop = new Properties()
    prop.load(file_stream)
    prop
  }
  val config: Properties = configTry match {
    case Failure(x) => {
      ic_logger.error(f"======> Failed to load ${getClass.getName} with path $global_properties_file_path with error ${x.getMessage}")
      throw x
    }
    case Success(x) => {
      ic_logger.info(f"======> Loaded successfully ${getClass.getName}")
      x
    }
  }

  lazy val spark_concurrent_threads               = config.getOrDefault("spark_concurrent_threads", "4").toString
  lazy val spark_shuffle_partitions               = config.getOrDefault("spark_shuffle_partitions", "1").toString
  lazy val spark_output_partition_overwrite_mode  = config.getOrDefault("spark_output_partition_overwrite_mode","dynamic").toString
  lazy val running_environment                    = config.getOrDefault("running_environment","local").toString
  
  lazy val gcp_project                            = sys.env.getOrElse("GCP_PROJECT", config.getProperty("gcp_project")) 
  lazy val gcp_project_key_name                   = sys.env.getOrElse("GCP_PROJECT_KEY_NAME", config.getProperty("gcp_project_key_name"))

  lazy val send_notification                      = sys.env.getOrElse("SEND_NOTIFICATION", config.getProperty("send_notification"))
  lazy val notification_level                     = sys.env.getOrElse("NOTIFICATION_LEVEL", config.getProperty("notification_level"))

  lazy val slack_webhook_url                      = sys.env.getOrElse("SLACK_WEBHOOK_URL", config.getProperty("slack_webhook_url"))
  lazy val slack_env                              = sys.env.getOrElse("SLACK_ENV", config.getProperty("slack_env"))

  lazy val log_db_url                             = sys.env.getOrElse("LOG_DB_URL", config.getProperty("log_db_url"))
  lazy val log_db_user                            = sys.env.getOrElse("LOG_DB_USER", config.getProperty("log_db_user"))
  lazy val log_db_pwd                             = sys.env.getOrElse("LOG_DB_PWD", config.getProperty("log_db_pwd"))
}
