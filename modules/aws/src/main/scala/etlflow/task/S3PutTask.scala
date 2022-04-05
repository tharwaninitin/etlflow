package etlflow.task

import etlflow.aws._
import zio.{RIO, ZIO}
import java.nio.file.Paths

case class S3PutTask(name: String, bucket: String, key: String, file: String, overwrite: Boolean)
    extends EtlTaskZIO[S3Env, Unit] {
  override protected def processZio: RIO[S3Env, Unit] = for {
    _    <- ZIO.succeed(logger.info("#" * 50))
    _    <- ZIO.succeed(logger.info(s"Input local path $file"))
    _    <- ZIO.succeed(logger.info(s"Output S3 path s3://$bucket/$key"))
    path <- ZIO.effect(Paths.get(file))
    _ <- S3Api
      .putObject(bucket, key, path, overwrite)
      .tapError(ex => ZIO.succeed(logger.error(ex.getMessage)))
    _ <- ZIO.succeed(logger.info(s"Successfully uploaded file $file in location s3://$bucket/$key"))
    _ <- ZIO.succeed(logger.info("#" * 100))
  } yield ()
}
