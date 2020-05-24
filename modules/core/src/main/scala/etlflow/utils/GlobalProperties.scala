package etlflow.utils

import java.io.FileInputStream
import java.util.Properties
import org.slf4j.LoggerFactory
import scala.util.{Failure, Success, Try}

abstract class GlobalProperties(global_properties_file_path: String, job_properties: Map[String,String] = Map.empty) {
  private val gp_logger = LoggerFactory.getLogger(getClass.getName)
  gp_logger.info(s"======> Trying to load ${getClass.getName} with path $global_properties_file_path")

  val configTry: Try[Properties] = Try {
    val file_stream = new FileInputStream(global_properties_file_path)
    val prop = new Properties()
    prop.load(file_stream)
    prop
  }
  val config: Properties = configTry match {
    case Failure(x) =>
      gp_logger.error(f"======> Failed to load ${getClass.getName} with path $global_properties_file_path with error ${x.getMessage}")
      throw x
    case Success(x) =>
      gp_logger.info(f"======> Loaded successfully ${getClass.getName}")
      x
  }

  lazy val spark_concurrent_threads: String = config.getOrDefault("spark_concurrent_threads", "4").toString
  lazy val spark_shuffle_partitions: String = config.getOrDefault("spark_shuffle_partitions", "1").toString
  lazy val spark_output_partition_overwrite_mode: String = config.getOrDefault("spark_output_partition_overwrite_mode","dynamic").toString
  lazy val running_environment: String      = config.getOrDefault("running_environment","local").toString
  
  lazy val gcp_project: String              = sys.env.getOrElse("GCP_PROJECT", config.getProperty("gcp_project"))
  lazy val gcp_credential_file_path: String = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", config.getProperty("gcp_credential_file_path"))
  lazy val gcp_region: String               = sys.env.getOrElse("GCP_REGION", config.getProperty("gcp_region"))
  lazy val gcp_dp_endpoint: String          = sys.env.getOrElse("GCP_DP_ENDPOINT", config.getProperty("gcp_dp_endpoint"))
  lazy val gcp_dp_cluster_name: String      = sys.env.getOrElse("GCP_DP_CLUSTER_NAME", config.getProperty("gcp_dp_cluster_name"))

  lazy val send_notification: String        = sys.env.getOrElse("SEND_NOTIFICATION", config.getProperty("send_notification"))
  lazy val notification_level: String       = sys.env.getOrElse("NOTIFICATION_LEVEL", config.getProperty("notification_level"))

  lazy val slack_webhook_url: String        = sys.env.getOrElse("SLACK_WEBHOOK_URL", config.getProperty("slack_webhook_url"))
  lazy val slack_env: String                = sys.env.getOrElse("SLACK_ENV", config.getProperty("slack_env"))

  lazy val log_db_driver: String            = sys.env.getOrElse("LOG_DB_DRIVER", config.getProperty("log_db_driver"))
  lazy val log_db_url: String               = sys.env.getOrElse("LOG_DB_URL", config.getProperty("log_db_url"))
  lazy val log_db_user: String              = sys.env.getOrElse("LOG_DB_USER", config.getProperty("log_db_user"))
  lazy val log_db_pwd: String               = sys.env.getOrElse("LOG_DB_PWD", config.getProperty("log_db_pwd"))
}
