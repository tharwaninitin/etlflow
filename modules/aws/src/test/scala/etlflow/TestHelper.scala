package etlflow

import software.amazon.awssdk.regions.Region

trait TestHelper {
  lazy val s3_bucket: String = "test"
  lazy val s3_path: String   = "temp/ratings.csv"
  lazy val s3_region: Region = Region.AP_SOUTH_1
  val canonical_path: String = new java.io.File(".").getCanonicalPath
  val local_file             = s"$canonical_path/modules/aws/src/test/resources/input/ratings.csv"
}
