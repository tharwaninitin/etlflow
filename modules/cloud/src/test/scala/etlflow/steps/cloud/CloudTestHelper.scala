package etlflow.steps.cloud

trait CloudTestHelper {

  lazy val gcs_bucket: String          = sys.env("GCS_BUCKET")
  lazy val s3_bucket: String           = sys.env("S3_BUCKET")
  lazy val s3_input_location: String   = sys.env("S3_INPUT_LOCATION")
  lazy val gcs_input_location: String  = sys.env("GCS_INPUT_LOCATION")
  lazy val gcs_output_location: String = sys.env("GCS_OUTPUT_LOCATION")
  lazy val s3_region: String           = "ap-south-1"
}
