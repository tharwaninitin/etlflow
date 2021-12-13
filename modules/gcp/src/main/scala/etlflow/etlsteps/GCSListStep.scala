package etlflow.etlsteps

import com.google.cloud.storage.Blob
import etlflow.gcp._
import etlflow.schema.Credential.GCP
import zio.Task

case class GCSListStep(
                        name: String,
                        bucket: String,
                        prefix: String,
                        credentials: Option[GCP] = None
                      ) extends EtlStep[Unit, List[Blob]] {
  override def process(input_state: => Unit): Task[List[Blob]] = {
    val env = GCS.live(credentials)
    logger.info(s"Listing files at $bucket/$prefix")
    GCSApi.listObjects(bucket, prefix).provideLayer(env)
  }

  override def getStepProperties: Map[String, String] =
    Map(
      "name" -> name,
      "bucket" -> bucket,
      "prefix" -> prefix
    )
}
