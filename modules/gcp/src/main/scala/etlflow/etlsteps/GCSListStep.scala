package etlflow.etlsteps

import com.google.cloud.storage.Blob
import etlflow.gcp._
import zio.RIO

case class GCSListStep(name: String, bucket: String, prefix: String) extends EtlStep[GCSEnv, List[Blob]] {

  override def process: RIO[GCSEnv, List[Blob]] = {
    logger.info(s"Listing files at $bucket/$prefix")
    GCSApi.listObjects(bucket, prefix)
  }

  override def getStepProperties: Map[String, String] = Map("name" -> name, "bucket" -> bucket, "prefix" -> prefix)
}
