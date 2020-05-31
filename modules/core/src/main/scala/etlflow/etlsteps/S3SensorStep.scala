package etlflow.etlsteps
import zio.{Schedule, Task}
import etlflow.aws._
import etlflow.utils.AWS
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import zio.clock.Clock
import zio.duration.{Duration => ZDuration}
import scala.concurrent.duration._

class S3SensorStep private[etlsteps](
                   val name: String,
                   bucket: => String,
                   prefix: => String,
                   key: => String,
                   retry: Int,
                   spaced: Duration,
                   region: Region,
                   endpoint_override: Option[String] = None,
                   credentials: Option[AWS] = None
                 ) extends EtlStep[Unit,Unit] {
  override def process(input_state: => Unit): Task[Unit] = {
    val env       = S3Client.live >>> S3Api.live
    val lookup    = lookupObject(bucket,prefix,key).provideLayer(env)

    def program(s3: S3AsyncClient): Task[Unit] = (for {
                    out <- lookup.provide(s3)
                    _   <- if(out) Task.succeed(etl_logger.info(s"Found key $key in s3 location s3://$bucket/$prefix/"))
                           else Task.fail(new RuntimeException(s"key $key not found in s3 location s3://$bucket/$prefix/"))
                  } yield ()).retry(schedule(retry,spaced)).provideLayer(Clock.live)

    val runnable  = for {
                      _   <- Task.succeed(etl_logger.info(s"Starting sensor for s3 location s3://$bucket/$prefix/$key"))
                      s3  <- createClient(region, endpoint_override, credentials)
                      _   <- program(s3)
                    } yield ()
    runnable
  }

  def schedule[A](retry: Int, spaced: Duration): Schedule[Clock, A, (Int, Int)] =
    Schedule.recurs(retry) && Schedule.spaced(ZDuration.fromScala(spaced)).onDecision((a: A, s) => s match {
      case None => Task.succeed(etl_logger.info(s"done trying"))
      case Some(att) => Task.succeed(etl_logger.info(s"attempt #$att"))
    })
}

object S3SensorStep {
  def apply(name: String,
            bucket: => String,
            prefix: => String,
            key: => String,
            retry: Int,
            spaced: Duration,
            region: Region,
            endpoint_override: Option[String] = None,
            credentials: Option[AWS] = None): S3SensorStep =
    new S3SensorStep(name, bucket, prefix, key, retry, spaced, region, endpoint_override, credentials)
}
