package etlflow.etlsteps

import etlflow.gcp._
import etlflow.model.Credential.GCP
import zio.{Task, ZIO}

case class GCSDeleteStep(
    name: String,
    bucket: String,
    prefix: String,
    parallelism: Int,
    credentials: Option[GCP] = None
) extends EtlStep[Unit, Unit] {
  override def process(input_state: => Unit): Task[Unit] = {
    val env = GCS.live(credentials)
    logger.info(s"Deleting files at $bucket/$prefix")
    for {
      list <- GCSApi.listObjects(bucket, prefix).provideLayer(env)
      _ <- ZIO.foreachParN_(parallelism)(list)(blob =>
        Task {
          logger.info(s"Deleting object ${blob.getName}")
          blob.delete()
        }
      )
    } yield ()
  }

  override def getStepProperties: Map[String, String] =
    Map(
      "name"        -> name,
      "bucket"      -> bucket,
      "prefix"      -> prefix,
      "parallelism" -> parallelism.toString
    )
}
