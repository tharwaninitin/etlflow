package etlflow.task

import gcp4zio.gcs._
import zio.RIO

case class GCSDeleteTask(name: String, bucket: String, prefix: String) extends EtlTask[GCS, Unit] {

  override protected def process: RIO[GCS, Unit] = {
    logger.info(s"Deleting file at gs://$bucket/$prefix")
    GCS.deleteObject(bucket, prefix).unit
  }

  override val metadata: Map[String, String] = Map("bucket" -> bucket, "prefix" -> prefix)
}
