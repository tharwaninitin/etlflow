package etlflow.etlsteps

import zio.{Schedule, Task}
import etlflow.gcp._
import etlflow.utils.GCP
import zio.clock.Clock
import zio.duration.{Duration => ZDuration}
import scala.concurrent.duration.Duration

class GCSSensorStep private [etlsteps](
               val name: String,
               bucket: => String,
               prefix: => String,
               key: => String,
               retry: Int,
               spaced: Duration,
               credentials: Option[GCP] = None
               ) extends EtlStep[Unit,Unit] {
  override def process(input_state: => Unit): Task[Unit] = {
    val env = GCSStorage.live(credentials)
    val program = lookupObject(bucket,prefix,key)
    val runnable = for {
                      _   <- Task.succeed(etl_logger.info(s"Starting sensor for GCS location gs://$bucket/$prefix/$key"))
                      out <- program.provideLayer(env)
                      _   <- if(out) Task.succeed(etl_logger.info(s"Found key $key in GCS location gs://$bucket/$prefix/"))
                             else Task.fail(new RuntimeException(s"key $key not found in GCS location gs://$bucket/$prefix/"))
                    } yield ()
    runnable.retry(schedule(retry,spaced)).provideLayer(Clock.live)
  }

  def schedule[A](retry: Int, spaced: Duration): Schedule[Clock, A, (Int, Int)] =
    Schedule.recurs(retry) && Schedule.spaced(ZDuration.fromScala(spaced)).onDecision((a: A, s) => s match {
      case None => Task.succeed(etl_logger.info(s"done trying"))
      case Some(att) => Task.succeed(etl_logger.info(s"attempt #$att"))
    })
}

object GCSSensorStep {
  def apply(name: String,
            bucket: => String,
            prefix: => String,
            key: => String,
            retry: Int,
            spaced: Duration,
            credentials: Option[GCP ]= None
           ): GCSSensorStep =
    new GCSSensorStep(name, bucket, prefix, key, retry, spaced, credentials)
}
