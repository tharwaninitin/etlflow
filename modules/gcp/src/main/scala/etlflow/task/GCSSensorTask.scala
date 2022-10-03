package etlflow.task

import gcp4zio.gcs._
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import zio.Clock
import zio._
import scala.concurrent.duration.Duration

case class GCSSensorTask(name: String, bucket: String, prefix: String, retry: Int, spaced: Duration)
    extends EtlTask[GCSEnv with Clock, Unit] {

  override protected def process: RIO[GCSEnv with Clock, Unit] = {
    val lookup = GCSApi.lookupObject(bucket, prefix)

    val program: RIO[GCSEnv with Clock, Unit] =
      (for {
         out <- lookup
         _ <-
           if (out)
             ZIO.succeed(logger.info(s"Found key gs://$bucket/$prefix in GCS"))
           else
             ZIO.fail(
               RetryException(s"Key gs://$bucket/$prefix not found in GCS")
             )
       } yield ()).retry(RetrySchedule(retry, spaced))

    val runnable = for {
      _ <- ZIO.succeed(logger.info(s"Starting sensor for GCS location gs://$bucket/$prefix"))
      _ <- program
    } yield ()

    runnable
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getTaskProperties: Map[String, String] =
    Map("bucket" -> bucket, "prefix" -> prefix, "retry" -> retry.toString, "spaced" -> spaced.toString)
}
