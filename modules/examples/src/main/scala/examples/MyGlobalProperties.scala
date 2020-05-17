package examples

import etlflow.utils.GlobalProperties

class MyGlobalProperties(val global_properties_file_path: String) extends GlobalProperties(global_properties_file_path) {

  lazy val gcs_output_bucket: String = sys.env.getOrElse("GCS_OUTPUT_BUCKET", config.getProperty("gcs_output_bucket"))
  lazy val jdbc_user: String = log_db_user
  lazy val jdbc_pwd: String = log_db_pwd
  lazy val jdbc_url: String = log_db_url
  lazy val jdbc_driver: String = sys.env.getOrElse("JDBC_DRIVER", "org.postgresql.Driver")

}