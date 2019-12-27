package etljob3

import etljobs.utils.Settings
import java.io.FileInputStream
import java.util.Properties

class MySettings(file_path: String) extends Settings(file_path) {
    private val file_stream = new FileInputStream(file_path)
    private val config = new Properties()
    config.load(file_stream)
    
    lazy val gcs_output_bucket = sys.env.getOrElse("GCS_OUTPUT_BUCKET", config.getProperty("gcs_output_bucket"))
}