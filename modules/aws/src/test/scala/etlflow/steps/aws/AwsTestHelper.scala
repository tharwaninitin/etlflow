package etlflow.steps.aws

import software.amazon.awssdk.regions.Region

trait AwsTestHelper {
  lazy val gcs_bucket: String        = sys.env("GCS_BUCKET")
  lazy val s3_bucket: String         = sys.env("S3_BUCKET")
  lazy val s3_input_location: String = sys.env("S3_INPUT_LOCATION")
  lazy val s3_region: Region         = Region.AP_SOUTH_1
  val canonical_path: String         = new java.io.File(".").getCanonicalPath
  val file = s"$canonical_path/modules/core/src/test/resources/input/movies/ratings_parquet/ratings.parquet"
}
