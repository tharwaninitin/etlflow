package etlflow.etlsteps

import etlflow.gcp._
import etlflow.model.Credential.GCP
import zio.Task

class GCSPutStep private[etlsteps] (
    val name: String,
    bucket: => String,
    key: => String,
    file: => String,
    credentials: Option[GCP] = None
) extends EtlStep[Any, Unit] {

  override def process: Task[Unit] = {
    val env     = GCS.live(credentials)
    val program = GCSApi.putObject(bucket, key, file)
    val runnable = for {
      _ <- Task.succeed(logger.info("#" * 100))
      _ <- Task.succeed(logger.info(s"Input local path $file"))
      _ <- Task.succeed(logger.info(s"Output GCS path gs://$bucket/$key"))
      _ <- program
        .provideLayer(env)
        .foldM(
          ex => Task.succeed(logger.error(ex.getMessage)) *> Task.fail(ex),
          _ => Task.succeed(logger.info(s"Successfully uploaded file $file in location gs://$bucket/$key"))
        )
      _ <- Task.succeed(logger.info("#" * 100))
    } yield ()
    runnable
  }

  override def getStepProperties: Map[String, String] =
    Map(
      "name"   -> name,
      "bucket" -> bucket,
      "key"    -> key,
      "file"   -> file
    )
}

object GCSPutStep {

  def apply(
      name: String,
      bucket: => String,
      key: => String,
      file: => String,
      credentials: Option[GCP] = None
  ): GCSPutStep =
    new GCSPutStep(name, bucket, key, file, credentials)
}
