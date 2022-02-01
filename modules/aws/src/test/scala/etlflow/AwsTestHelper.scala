package etlflow

import software.amazon.awssdk.regions.Region

trait AwsTestHelper {
  lazy val s3_bucket: String = "test"
  lazy val s3_region: Region = Region.AP_SOUTH_1
  val canonical_path: String = new java.io.File(".").getCanonicalPath
  val file                   = s"$canonical_path/modules/aws/src/test/resources/input/ratings.parquet"
}
