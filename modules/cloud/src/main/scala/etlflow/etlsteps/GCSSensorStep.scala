package etlflow.etlsteps

import etlflow.gcp._
import etlflow.schema.Credential.GCP
import etlflow.utils.EtlflowError.EtlJobException
import zio.Task
import zio.clock.Clock

import scala.concurrent.duration.Duration

class GCSSensorStep private [etlsteps](
               val name: String,
               bucket: => String,
               prefix: => String,
               key: => String,
               retry: Int,
               spaced: Duration,
               credentials: Option[GCP] = None
               ) extends EtlStep[Unit,Unit] with SensorStep {
  override def process(input_state: => Unit): Task[Unit] = {
    val env     = GCS.live(credentials)
    val lookup  = GCSService.lookupObject(bucket,prefix,key).provideLayer(env)

    val program: Task[Unit] = (for {
                                out <- lookup
                                _   <- if(out) Task.succeed(logger.info(s"Found key $key in GCS location gs://$bucket/$prefix/"))
                                       else Task.fail(EtlJobException(s"key $key not found in GCS location gs://$bucket/$prefix/"))
                              } yield ()).retry(noThrowable && schedule(retry,spaced)).provideLayer(Clock.live)

    val runnable = for {
                      _   <- Task.succeed(logger.info(s"Starting sensor for GCS location gs://$bucket/$prefix/$key"))
                      _   <- program
                    } yield ()

    runnable
  }
}

object GCSSensorStep {
  def apply(name: String,
            bucket: => String,
            prefix: => String,
            key: => String,
            retry: Int,
            spaced: Duration,
            credentials: Option[GCP]= None
           ): GCSSensorStep =
    new GCSSensorStep(name, bucket, prefix, key, retry, spaced, credentials)
}
