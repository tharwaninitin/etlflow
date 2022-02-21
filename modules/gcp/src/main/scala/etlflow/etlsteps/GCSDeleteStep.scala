package etlflow.etlsteps

import gcp4zio._
import zio.RIO

case class GCSDeleteStep(
    name: String,
    bucket: String,
    prefix: String
) extends EtlStep[GCSEnv, Unit] {

  override def process: RIO[GCSEnv, Unit] = {
    logger.info(s"Deleting file at gs://$bucket/$prefix")
    GCSApi.deleteObject(bucket, prefix).unit
  }

  override def getStepProperties: Map[String, String] = Map("bucket" -> bucket, "prefix" -> prefix)
}
