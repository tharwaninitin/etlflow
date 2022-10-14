package etlflow.task

import etlflow.aws._
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import zio.{Clock, RIO, ZIO}
import scala.concurrent.duration._

case class S3SensorTask(name: String, bucket: String, key: String, retry: Int, spaced: Duration)
    extends EtlTask[S3Env with Clock, Unit] {
  override protected def process: RIO[S3Env with Clock, Unit] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Starting sensor for s3 location s3://$bucket/$key")
    _ <- S3Api
      .lookupObject(bucket, key)
      .flatMap { out =>
        if (out) ZIO.logInfo(s"Found key $key in s3 bucket s3://$bucket")
        else ZIO.fail(RetryException(s"key $key not found in s3 bucket s3://$bucket"))
      }
      .retry(RetrySchedule(retry, spaced))
  } yield ()
}
