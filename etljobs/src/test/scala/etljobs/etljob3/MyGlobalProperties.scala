package etljobs.etljob3

import etljobs.utils.GlobalProperties

class MyGlobalProperties(val global_properties_file_path: String) extends GlobalProperties(global_properties_file_path) {

  lazy val gcs_output_bucket = sys.env.getOrElse("GCS_OUTPUT_BUCKET", config.getProperty("gcs_output_bucket"))
}