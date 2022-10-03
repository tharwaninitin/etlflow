package etlflow.task

import etlflow.aws._
import zio.{RIO, ZIO}
import java.nio.file.Paths

case class S3PutTask(name: String, bucket: String, key: String, file: String, overwrite: Boolean) extends EtlTask[S3Env, Unit] {
  override protected def process: RIO[S3Env, Unit] = for {
    _    <- ZIO.logInfo("#" * 50)
    _    <- ZIO.logInfo(s"Input local path $file")
    _    <- ZIO.logInfo(s"Output S3 path s3://$bucket/$key")
    path <- ZIO.attempt(Paths.get(file))
    _ <- S3Api
      .putObject(bucket, key, path, overwrite)
      .tapError(ex => ZIO.logError(ex.getMessage))
    _ <- ZIO.logInfo(s"Successfully uploaded file $file in location s3://$bucket/$key")
    _ <- ZIO.logInfo("#" * 100)
  } yield ()
}
