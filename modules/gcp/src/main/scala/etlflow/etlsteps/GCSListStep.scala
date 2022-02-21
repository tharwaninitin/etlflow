package etlflow.etlsteps

import com.google.cloud.storage.Blob
import gcp4zio._
import zio.{Chunk, RIO}

case class GCSListStep(name: String, bucket: String, prefix: Option[String], recursive: Boolean)
    extends EtlStep[GCSEnv, Chunk[Blob]] {

  override def process: RIO[GCSEnv, Chunk[Blob]] = {
    logger.info(s"Listing files at $bucket/$prefix")
    GCSApi.listObjects(bucket, prefix, recursive).runCollect
  }

  override def getStepProperties: Map[String, String] =
    Map("bucket" -> bucket, "prefix" -> prefix.getOrElse(""), "recursive" -> recursive.toString)
}
