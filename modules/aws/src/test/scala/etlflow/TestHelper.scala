package etlflow

import software.amazon.awssdk.regions.Region

trait TestHelper {
  lazy val s3Bucket: String = "test"
  lazy val s3Path: String   = "temp/ratings.csv"
  lazy val s3Region: Region = Region.AP_SOUTH_1
  val canonicalPath: String = new java.io.File(".").getCanonicalPath
  val localFile             = s"$canonicalPath/modules/aws/src/test/resources/input/ratings.csv"
}
