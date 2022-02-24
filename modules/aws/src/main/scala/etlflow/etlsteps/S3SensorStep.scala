package etlflow.etlsteps

import etlflow.aws._
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import zio.clock.Clock
import zio.{IO, RIO, UIO}
import scala.concurrent.duration._

case class S3SensorStep(name: String, bucket: String, key: String, retry: Int, spaced: Duration)
    extends EtlStep[S3Env with Clock, Unit] {

  protected def process: RIO[S3Env with Clock, Unit] = for {
    _ <- UIO(logger.info("#" * 50))
    _ <- UIO(logger.info(s"Starting sensor for s3 location s3://$bucket/$key"))
    _ <- S3Api
      .lookupObject(bucket, key)
      .flatMap { out =>
        if (out) UIO(logger.info(s"Found key $key in s3 bucket s3://$bucket"))
        else IO.fail(RetryException(s"key $key not found in s3 bucket s3://$bucket"))
      }
      .retry(RetrySchedule(retry, spaced))
  } yield ()
}
