package etlflow.task

import com.google.cloud.storage.Blob
import gcp4zio.gcs._
import zio.{Chunk, RIO}

case class GCSListTask(name: String, bucket: String, prefix: Option[String], recursive: Boolean)
    extends EtlTask[GCS, Chunk[Blob]] {

  override protected def process: RIO[GCS, Chunk[Blob]] = {
    logger.info(s"Listing files at $bucket/$prefix")
    GCS.listObjects(bucket, prefix, recursive).runCollect
  }

  override val metadata: Map[String, String] =
    Map("bucket" -> bucket, "prefix" -> prefix.getOrElse(""), "recursive" -> recursive.toString)
}
