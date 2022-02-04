package etlflow.etlsteps

import gcp4zio._
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import zio.clock.Clock
import zio.{IO, RIO, Task, UIO}
import scala.concurrent.duration.Duration

case class GCSSensorStep(
    name: String,
    bucket: String,
    prefix: String,
    key: String,
    retry: Int,
    spaced: Duration
) extends EtlStep[GCSEnv with Clock, Unit] {

  override def process: RIO[GCSEnv with Clock, Unit] = {
    val lookup = GCSApi.lookupObject(bucket, prefix, key)

    val program: RIO[GCSEnv with Clock, Unit] =
      (for {
         out <- lookup
         _ <-
           if (out)
             UIO(logger.info(s"Found key $key in GCS location gs://$bucket/$prefix/"))
           else
             IO.fail(
               RetryException(s"key $key not found in GCS location gs://$bucket/$prefix/")
             )
       } yield ()).retry(RetrySchedule(retry, spaced))

    val runnable = for {
      _ <- Task.succeed(logger.info(s"Starting sensor for GCS location gs://$bucket/$prefix/$key"))
      _ <- program
    } yield ()

    runnable
  }

  override def getStepProperties: Map[String, String] = Map(
    "bucket" -> bucket,
    "prefix" -> prefix,
    "key"    -> key,
    "retry"  -> retry.toString,
    "spaced" -> spaced.toString
  )
}
