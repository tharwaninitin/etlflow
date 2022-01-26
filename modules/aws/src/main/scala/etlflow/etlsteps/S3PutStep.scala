package etlflow.etlsteps

import etlflow.aws._
import zio.{RIO, UIO}

case class S3PutStep(name: String, bucket: String, key: String, file: String) extends EtlStep[S3Env, Unit] {

  override def process: RIO[S3Env, Unit] = for {
    _ <- UIO(logger.info("#" * 100))
    _ <- UIO(logger.info(s"Input local path $file"))
    _ <- UIO(logger.info(s"Output S3 path s3://$bucket/$key"))
    _ <- S3Api
      .putObject(bucket, key, file)
      .tapError(ex => UIO(logger.error(ex.getMessage)))
    _ <- UIO(logger.info(s"Successfully uploaded file $file in location s3://$bucket/$key"))
    _ <- UIO(logger.info("#" * 100))
  } yield ()
}
