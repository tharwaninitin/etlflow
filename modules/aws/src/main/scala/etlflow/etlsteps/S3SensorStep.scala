package etlflow.etlsteps

import etlflow.aws._
import etlflow.model.Credential.AWS
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import zio.{IO, RIO, Task, UIO}
import zio.clock.Clock
import scala.concurrent.duration._

class S3SensorStep private[etlsteps] (
    val name: String,
    bucket: => String,
    prefix: => String,
    key: => String,
    retry: Int,
    spaced: Duration,
    region: Region,
    endpoint_override: Option[String] = None,
    credentials: Option[AWS] = None
) extends EtlStep[Clock, Unit] {

  override def process: RIO[Clock, Unit] = {
    val lookup = S3Api.lookupObject(bucket, prefix, key)
    def program(s3: S3AsyncClient): RIO[Clock, Unit] =
      (for {
         out <- lookup.provide(s3)
         _ <-
           if (out)
             UIO(
               logger.info(
                 s"Found key $key in s3 location s3://$bucket/$prefix/"
               )
             )
           else
             IO.fail(
               RetryException(
                 s"key $key not found in s3 location s3://$bucket/$prefix/"
               )
             )
       } yield ()).retry(RetrySchedule(retry, spaced))

    val runnable = for {
      _  <- Task.succeed(logger.info(s"Starting sensor for s3 location s3://$bucket/$prefix/$key"))
      s3 <- S3Api.createClient(region, endpoint_override, credentials)
      _  <- program(s3)
    } yield ()
    runnable
  }
}

object S3SensorStep {
  def apply(
      name: String,
      bucket: => String,
      prefix: => String,
      key: => String,
      retry: Int,
      spaced: Duration,
      region: Region,
      endpoint_override: Option[String] = None,
      credentials: Option[AWS] = None
  ): S3SensorStep =
    new S3SensorStep(name, bucket, prefix, key, retry, spaced, region, endpoint_override, credentials)
}
