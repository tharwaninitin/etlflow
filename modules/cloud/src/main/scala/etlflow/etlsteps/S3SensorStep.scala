package etlflow.etlsteps

import etlflow.aws._
import etlflow.schema.Credential.AWS
import etlflow.utils.EtlflowError.EtlJobException
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import zio.Task
import zio.clock.Clock

import scala.concurrent.duration._

class S3SensorStep private[etlsteps](
                   val name: String,
                   bucket: => String,
                   prefix: => String,
                   key: => String,
                   retry: Int,
                   spaced: Duration,
                   region: Region,
                   endpoint_override: Option[String] = None,
                   credentials: Option[AWS] = None
                 ) extends EtlStep[Unit,Unit] with SensorStep {
  override def process(input_state: => Unit): Task[Unit] = {
    val lookup = S3Api.lookupObject(bucket,prefix,key)
    def program(s3: S3AsyncClient): Task[Unit] = (for {
                    out <- lookup.provide(s3)
                    _   <- if(out) Task.succeed(logger.info(s"Found key $key in s3 location s3://$bucket/$prefix/"))
                           else Task.fail(EtlJobException(s"key $key not found in s3 location s3://$bucket/$prefix/"))
                  } yield ()).retry(noThrowable && schedule(retry,spaced)).provideLayer(Clock.live)

    val runnable  = for {
                      _   <- Task.succeed(logger.info(s"Starting sensor for s3 location s3://$bucket/$prefix/$key"))
                      s3  <- S3Api.createClient(region, endpoint_override, credentials)
                      _   <- program(s3)
                    } yield ()
    runnable
  }
}

object S3SensorStep {
  def apply(name: String,
            bucket: => String,
            prefix: => String,
            key: => String,
            retry: Int,
            spaced: Duration,
            region: Region,
            endpoint_override: Option[String] = None,
            credentials: Option[AWS] = None): S3SensorStep =
    new S3SensorStep(name, bucket, prefix, key, retry, spaced, region, endpoint_override, credentials)
}
