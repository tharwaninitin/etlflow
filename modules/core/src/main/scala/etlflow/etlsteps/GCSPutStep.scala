package etlflow.etlsteps

import etlflow.gcp._
import etlflow.utils.GCP
import zio.Task

class GCSPutStep private[etlsteps](
                   val name: String,
                   bucket: => String,
                   key: => String,
                   file: => String,
                   credentials: Option[GCP] = None
                 ) extends EtlStep[Unit,Unit] {
  override def process(input_state: => Unit): Task[Unit] = {
    val env       = GCSStorage.live(credentials)
    val program   = putObject(bucket,key,file)
    val runnable  = for {
                      _   <- Task.succeed(etl_logger.info(s"Starting upload for file $file in location gs://$bucket/$key"))
                      _   <- program.provideLayer(env).foldM(
                              ex => Task.succeed(etl_logger.error(ex.getMessage)) *> Task.fail(ex),
                              _  => Task.succeed(etl_logger.info(s"Successfully uploaded file $file in location gs://$bucket/$key"))
                            )
                    } yield ()
    runnable
  }
}

object GCSPutStep {

  def apply(name: String,
            bucket: => String,
            key: => String,
            file: => String,
            credentials: Option[GCP] = None
           ): GCSPutStep =
    new GCSPutStep(name, bucket, key, file, credentials)
}




