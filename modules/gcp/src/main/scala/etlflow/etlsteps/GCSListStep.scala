package etlflow.etlsteps

import com.google.cloud.storage.Blob
import etlflow.gcp._
import etlflow.schema.Credential.GCP
import zio.Task

case class GCSListStep(
                        name: String
                        , src_bucket: String
                        , src_prefix: String
                        , credentials: Option[GCP] = None
                      ) extends EtlStep[Unit, List[Blob]] {
  override def process(input_state: => Unit): Task[List[Blob]] = {
    val env = GCS.live(credentials)
    logger.info(s"Listing files at $src_bucket/$src_prefix")
    GCSApi.listObjects(src_bucket, src_prefix).provideLayer(env)
  }
}
