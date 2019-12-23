package etljobs.utils

import java.io.FileInputStream
import java.util.Properties

class Settings(filepath:String) {
  private val file_path = filepath
  private val file_stream = new FileInputStream(file_path)
  private val config = new Properties()

  config.load(file_stream)

  lazy val spark_concurrent_threads               = config.getProperty("spark_concurrent_threads")
  lazy val spark_shuffle_partitions               = config.getProperty("spark_shuffle_partitions")
  lazy val spark_output_partition_overwrite_mode  = config.getProperty("spark_output_partition_overwrite_mode")
  lazy val running_environment                    = config.getProperty("running_environment")
  
  lazy val gcp_project                            = sys.env.getOrElse("GCP_PROJECT", config.getProperty("gcp_project")) 
  lazy val gcp_project_key_name                   = sys.env.getOrElse("GCP_PROJECT_KEY_NAME", config.getProperty("gcp_project_key_name"))
  lazy val gcs_output_bucket                      = sys.env.getOrElse("GCS_OUTPUT_BUCKET", config.getProperty("gcs_output_bucket"))
  
  lazy val slack_webhook_url                      = sys.env.getOrElse("SLACK_WEBHOOK_URL", config.getProperty("slack_webhook_url"))
  lazy val slack_env                              = sys.env.getOrElse("SLACK_ENV", config.getProperty("slack_env"))
}
