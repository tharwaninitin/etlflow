package etlflow.task

import gcp4zio.gcs._
import zio.RIO

case class GCSDeleteTask(name: String, bucket: String, prefix: String) extends EtlTask[GCSEnv, Unit] {

  override protected def process: RIO[GCSEnv, Unit] = {
    logger.info(s"Deleting file at gs://$bucket/$prefix")
    GCSApi.deleteObject(bucket, prefix).unit
  }

  override def getTaskProperties: Map[String, String] = Map("bucket" -> bucket, "prefix" -> prefix)
}
