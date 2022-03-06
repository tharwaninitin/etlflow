package etlflow

package object gcp {

  sealed trait FSType
  object FSType {
    case object LOCAL extends FSType
    case object GCS   extends FSType
  }

  sealed trait Location
  object Location {
    case class LOCAL(path: String)               extends Location
    case class GCS(bucket: String, path: String) extends Location
  }
}
