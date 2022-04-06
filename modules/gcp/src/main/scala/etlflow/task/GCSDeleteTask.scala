package etlflow.task

import gcp4zio._
import zio.RIO

case class GCSDeleteTask(name: String, bucket: String, prefix: String) extends EtlTaskZIO[GCSEnv, Unit] {

  override protected def processZio: RIO[GCSEnv, Unit] = {
    logger.info(s"Deleting file at gs://$bucket/$prefix")
    GCSApi.deleteObject(bucket, prefix).unit
  }

  override def getTaskProperties: Map[String, String] = Map("bucket" -> bucket, "prefix" -> prefix)
}
