package etljobs

import etljobs.utils.GlobalProperties

class MyGlobalProperties(val global_properties_file_path: String) extends GlobalProperties(global_properties_file_path) {

  lazy val gcs_output_bucket = sys.env.getOrElse("GCS_OUTPUT_BUCKET", config.getProperty("gcs_output_bucket"))
  lazy val jdbc_user = log_db_user
  lazy val jdbc_pwd = log_db_pwd
  lazy val jdbc_url = log_db_url
  lazy val jdbc_driver = sys.env.getOrElse("JDBC_DRIVER", "org.postgresql.Driver")
}