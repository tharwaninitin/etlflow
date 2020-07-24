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

  lazy val running_environment: String      = config.getOrDefault("running_environment","local").toString
  lazy val spark_concurrent_threads: String = config.getOrDefault("spark_concurrent_threads", "4").toString
  lazy val spark_shuffle_partitions: String = config.getOrDefault("spark_shuffle_partitions", "1").toString
  lazy val spark_output_partition_overwrite_mode: String = config.getOrDefault("spark_output_partition_overwrite_mode","dynamic").toString
  lazy val spark_scheduler_mode: String     = config.getOrDefault("spark_scheduler_mode", "FAIR").toString

  lazy val aws_access_key: String           = sys.env.getOrElse("ACCESS_KEY", config.getOrDefault("aws_access_key","").toString)
  lazy val aws_secret_key: String           = sys.env.getOrElse("SECRET_KEY", config.getOrDefault("aws_secret_key","").toString)

  lazy val gcp_project: String              = sys.env.getOrElse("GCP_PROJECT", config.getOrDefault("gcp_project","").toString)
  lazy val gcp_credential_file_path: String = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", config.getOrDefault("gcp_credential_file_path","").toString)
  lazy val gcp_region: String               = sys.env.getOrElse("GCP_REGION", config.getOrDefault("gcp_region","").toString)
  lazy val gcp_dp_endpoint: String          = sys.env.getOrElse("GCP_DP_ENDPOINT", config.getOrDefault("gcp_dp_endpoint","").toString)
  lazy val gcp_dp_cluster_name: String      = sys.env.getOrElse("GCP_DP_CLUSTER_NAME", config.getOrDefault("gcp_dp_cluster_name","").toString)
  lazy val main_class: String               = sys.env.getOrElse("MAIN_CLASS", config.getOrDefault("main_class","").toString)
  lazy val dep_libs: String                 = sys.env.getOrElse("DEP_LIBS", config.getOrDefault("dep_libs","").toString)

  lazy val send_notification: String        = sys.env.getOrElse("SEND_NOTIFICATION", config.getOrDefault("send_notification","").toString)
  lazy val notification_level: String       = sys.env.getOrElse("NOTIFICATION_LEVEL", config.getOrDefault("notification_level","").toString)

  lazy val slack_webhook_url: String        = sys.env.getOrElse("SLACK_WEBHOOK_URL", config.getOrDefault("slack_webhook_url","").toString)
  lazy val slack_env: String                = sys.env.getOrElse("SLACK_ENV", config.getOrDefault("slack_env","").toString)

  lazy val log_db_driver: String            = sys.env.getOrElse("LOG_DB_DRIVER", config.getOrDefault("log_db_driver","").toString)
  lazy val log_db_url: String               = sys.env.getOrElse("LOG_DB_URL", config.getOrDefault("log_db_url","").toString)
  lazy val log_db_user: String              = sys.env.getOrElse("LOG_DB_USER", config.getOrDefault("log_db_user","").toString)
  lazy val log_db_pwd: String               = sys.env.getOrElse("LOG_DB_PWD", config.getOrDefault("log_db_pwd","").toString)
}
