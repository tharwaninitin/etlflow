package etlflow.etlsteps

import gcp4zio._
import zio.{RIO, Task, UIO}
import java.nio.file.Paths

case class GCSPutStep(name: String, bucket: String, prefix: String, file: String) extends EtlStep[GCSEnv, Unit] {

  protected def process: RIO[GCSEnv, Unit] = for {
    _    <- UIO(logger.info("#" * 100))
    _    <- UIO(logger.info(s"Input local path $file"))
    path <- Task(Paths.get(file))
    _    <- UIO(logger.info(s"Output GCS path gs://$bucket/$prefix"))
    _ <- GCSApi
      .putObject(bucket, prefix, path, List.empty)
      .as(UIO(logger.info(s"Successfully uploaded file $file in location gs://$bucket/$prefix")))
      .tapError(ex => UIO(logger.error(ex.getMessage)))
    _ <- UIO(logger.info("#" * 100))
  } yield ()

  override def getStepProperties: Map[String, String] = Map("bucket" -> bucket, "prefix" -> prefix, "file" -> file)
}
