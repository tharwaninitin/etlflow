package etlflow.task

import gcp4zio._
import zio.{RIO, Task, UIO}
import java.nio.file.Paths

case class GCSPutTask(name: String, bucket: String, prefix: String, file: String) extends EtlTask[GCSEnv, Unit] {

  override protected def process: RIO[GCSEnv, Unit] = for {
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

  override def getTaskProperties: Map[String, String] = Map("bucket" -> bucket, "prefix" -> prefix, "file" -> file)
}
