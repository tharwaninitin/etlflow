package etlflow.etltask

import gcp4zio._
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import zio.clock.Clock
import zio._
import scala.concurrent.duration.Duration

case class GCSSensorTask(name: String, bucket: String, prefix: String, retry: Int, spaced: Duration)
    extends EtlTaskZIO[GCSEnv with Clock, Unit] {

  override protected def processZio: RIO[GCSEnv with Clock, Unit] = {
    val lookup = GCSApi.lookupObject(bucket, prefix)

    val program: RIO[GCSEnv with Clock, Unit] =
      (for {
         out <- lookup
         _ <-
           if (out)
             UIO(logger.info(s"Found key gs://$bucket/$prefix in GCS"))
           else
             IO.fail(
               RetryException(s"Key gs://$bucket/$prefix not found in GCS")
             )
       } yield ()).retry(RetrySchedule(retry, spaced))

    val runnable = for {
      _ <- Task.succeed(logger.info(s"Starting sensor for GCS location gs://$bucket/$prefix"))
      _ <- program
    } yield ()

    runnable
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getStepProperties: Map[String, String] =
    Map("bucket" -> bucket, "prefix" -> prefix, "retry" -> retry.toString, "spaced" -> spaced.toString)
}
