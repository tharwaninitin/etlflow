package etlflow.etlsteps

import etlflow.aws._
import etlflow.schema.Credential.AWS
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
    val program   = S3Api.putObject(bucket,key,file)
    val runnable  = for {
                      _   <- Task.succeed(logger.info("#"*100))
                      _   <- Task.succeed(logger.info(s"Input local path $file"))
                      _   <- Task.succeed(logger.info(s"Output S3 path s3://$bucket/$key"))
                      s3  <- S3Api.createClient(region, endpoint_override, credentials)
                      _   <- program.provide(s3).foldM(
                              ex => Task.succeed(logger.error(ex.getMessage)) *> Task.fail(ex),
                              _  => Task.succeed(logger.info(s"Successfully uploaded file $file in location s3://$bucket/$key"))
                            )
                      _   <- Task.succeed(logger.info("#"*100))
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


