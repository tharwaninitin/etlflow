package etlflow

package object gcp {

  sealed trait Location
  object Location {
    final case class LOCAL(path: String)               extends Location
    final case class GCS(bucket: String, path: String) extends Location
  }
}
