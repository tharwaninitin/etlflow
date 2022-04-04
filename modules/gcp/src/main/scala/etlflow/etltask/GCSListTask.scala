package etlflow.etltask

import com.google.cloud.storage.Blob
import gcp4zio._
import zio.{Chunk, RIO}

case class GCSListTask(name: String, bucket: String, prefix: Option[String], recursive: Boolean)
    extends EtlTaskZIO[GCSEnv, Chunk[Blob]] {

  override protected def processZio: RIO[GCSEnv, Chunk[Blob]] = {
    logger.info(s"Listing files at $bucket/$prefix")
    GCSApi.listObjects(bucket, prefix, recursive).runCollect
  }

  override def getStepProperties: Map[String, String] =
    Map("bucket" -> bucket, "prefix" -> prefix.getOrElse(""), "recursive" -> recursive.toString)
}
