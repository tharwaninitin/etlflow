package etlflow.etlsteps

import etlflow.aws._
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import zio.clock.Clock
import zio.{IO, RIO, Task, UIO}
import scala.concurrent.duration._

case class S3SensorStep(name: String, bucket: String, prefix: String, key: String, retry: Int, spaced: Duration)
    extends EtlStep[S3Env with Clock, Unit] {

  override def process: RIO[S3Env with Clock, Unit] = for {
    _ <- Task.succeed(logger.info(s"Starting sensor for s3 location s3://$bucket/$prefix/$key"))
    _ <- S3Api
      .lookupObject(bucket, prefix, key)
      .flatMap { out =>
        if (out) UIO(logger.info(s"Found key $key in s3 location s3://$bucket/$prefix/"))
        else IO.fail(RetryException(s"key $key not found in s3 location s3://$bucket/$prefix/"))
      }
      .retry(RetrySchedule(retry, spaced))
  } yield ()
}
