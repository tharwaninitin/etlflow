package etlflow

import etlflow.model.Credential

package object cloud {
  sealed trait Location {
    val bucket: String
    val location: String
  }
  object Location {
    case class LOCAL(override val location: String, override val bucket: String = "localhost") extends Location
    case class GCS(override val bucket: String, override val location: String, credentials: Option[Credential.GCP] = None)
        extends Location
    case class S3(
        override val bucket: String,
        override val location: String,
        region: String,
        credentials: Option[Credential.AWS] = None
    ) extends Location
  }
}
