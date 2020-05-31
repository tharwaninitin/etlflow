package etlflow.etlsteps

import etlflow.aws._
import etlflow.utils.AWS
import software.amazon.awssdk.regions.Region
import zio.Task

class S3PutStep private[etlsteps](
                   val name: String,
                   bucket: => String,
                   key: => String,
                   file: => String,
                   region: Region,
                   endpoint_override: Option[String] = None,
                   credentials: Option[AWS] = None
                 ) extends EtlStep[Unit,Unit] {
  override def process(input_state: => Unit): Task[Unit] = {
    val env       = S3Client.live >>> S3Api.live
    val program   = putObject(bucket,key,file)
    val runnable  = for {
                      _   <- Task.succeed(etl_logger.info(s"Starting upload for file $file in location s3://$bucket/$key"))
                      s3  <- createClient(region, endpoint_override, credentials)
                      _   <- program.provideLayer(env).provide(s3).foldM(
                              ex => Task.succeed(etl_logger.error(ex.getMessage)) *> Task.fail(ex),
                              _  => Task.succeed(etl_logger.info(s"Successfully uploaded file $file in location s3://$bucket/$key"))
                            )
                    } yield ()
    runnable
  }
}

object S3PutStep {
  def apply(name: String,
            bucket: => String,
            key: => String,
            file: => String,
            region: Region,
            endpoint_override: Option[String] = None,
            credentials: Option[AWS] = None
           ): S3PutStep =
    new S3PutStep(name, bucket, key, file, region, endpoint_override, credentials)
}


