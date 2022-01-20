package etlflow.etlsteps

import etlflow.gcp._
import etlflow.model.Credential.GCP
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import zio.{IO, RIO, Task, UIO}
import zio.clock.Clock
import scala.concurrent.duration.Duration

class GCSSensorStep private[etlsteps] (
    val name: String,
    bucket: => String,
    prefix: => String,
    key: => String,
    retry: Int,
    spaced: Duration,
    credentials: Option[GCP] = None
) extends EtlStep[Unit, Unit] {
  override def process(input_state: => Unit): RIO[Clock, Unit] = {
    val env    = GCS.live(credentials)
    val lookup = GCSApi.lookupObject(bucket, prefix, key).provideLayer(env)

    val program: RIO[Clock, Unit] = (for {
      out <- lookup
      _ <-
        if (out) UIO(logger.info(s"Found key $key in GCS location gs://$bucket/$prefix/"))
        else IO.fail(RetryException(s"key $key not found in GCS location gs://$bucket/$prefix/"))
    } yield ()).retry(RetrySchedule(retry, spaced))

    val runnable = for {
      _ <- Task.succeed(logger.info(s"Starting sensor for GCS location gs://$bucket/$prefix/$key"))
      _ <- program
    } yield ()

    runnable
  }
}

object GCSSensorStep {
  def apply(
      name: String,
      bucket: => String,
      prefix: => String,
      key: => String,
      retry: Int,
      spaced: Duration,
      credentials: Option[GCP] = None
  ): GCSSensorStep =
    new GCSSensorStep(name, bucket, prefix, key, retry, spaced, credentials)
}
