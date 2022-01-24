package etlflow.etlsteps

import etlflow.gcp._
import zio.{RIO, UIO}

case class GCSPutStep(name: String, bucket: String, key: String, file: String) extends EtlStep[GCSEnv, Unit] {

  override def process: RIO[GCSEnv, Unit] = {
    val program = GCSApi.putObject(bucket, key, file)
    val runnable = for {
      _ <- UIO(logger.info("#" * 100))
      _ <- UIO(logger.info(s"Input local path $file"))
      _ <- UIO(logger.info(s"Output GCS path gs://$bucket/$key"))
      _ <- program
        .as(UIO(logger.info(s"Successfully uploaded file $file in location gs://$bucket/$key")))
        .tapError(ex => UIO(logger.error(ex.getMessage)))
      _ <- UIO(logger.info("#" * 100))
    } yield ()
    runnable
  }

  override def getStepProperties: Map[String, String] = Map(
    "name"   -> name,
    "bucket" -> bucket,
    "key"    -> key,
    "file"   -> file
  )
}
