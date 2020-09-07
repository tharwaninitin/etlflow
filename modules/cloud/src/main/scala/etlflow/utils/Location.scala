package etlflow.utils

import software.amazon.awssdk.regions.Region

sealed trait Location {
  val location: String
}
object Location {
  case class LOCAL(override val location: String) extends Location
  case class GCS(override val location: String, credentials: Option[Environment.GCP] = None) extends Location
  case class S3(override val location: String, region: Region, credentials: Option[Environment.AWS] = None) extends Location
}

