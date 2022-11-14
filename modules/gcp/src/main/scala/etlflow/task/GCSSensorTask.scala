package etlflow.task

import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import gcp4zio.gcs._
import zio._
import scala.concurrent.duration.Duration

case class GCSSensorTask(name: String, bucket: String, prefix: String, retry: Int, spaced: Duration) extends EtlTask[GCS, Unit] {

  override protected def process: RIO[GCS, Unit] = {
    val lookup = GCS.lookupObject(bucket, prefix)

    val program: RIO[GCS, Unit] = for {
      out <- lookup
      _ <-
        if (out) ZIO.succeed(logger.info(s"Found key gs://$bucket/$prefix in GCS"))
        else ZIO.fail(RetryException(s"Key gs://$bucket/$prefix not found in GCS"))
    } yield ()

    val runnable: RIO[GCS, Unit] = for {
      _ <- ZIO.succeed(logger.info(s"Starting sensor for GCS location gs://$bucket/$prefix"))
      _ <- program.retry(RetrySchedule.recurs(retry, spaced))
    } yield ()

    runnable
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getTaskProperties: Map[String, String] =
    Map("bucket" -> bucket, "prefix" -> prefix, "retry" -> retry.toString, "spaced" -> spaced.toString)
}
