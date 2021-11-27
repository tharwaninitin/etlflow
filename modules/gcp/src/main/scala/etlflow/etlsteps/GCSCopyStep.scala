package etlflow.etlsteps

import etlflow.gcp._
import etlflow.schema.Credential.GCP
import zio.Task

case class GCSCopyStep(
                   name: String
                   ,src_bucket: String
                   ,src_prefix: String
                   ,target_bucket: String
                   ,target_prefix: String
                   ,parallelism: Int
                   ,overwrite: Boolean = true
                   ,credentials: Option[GCP] = None
                 ) extends EtlStep[Unit,Unit] {
  override def process(input_state: => Unit): Task[Unit] = {
    val env       = GCS.live(credentials)
    val program   = GCSApi.copyObjects(src_bucket,src_prefix,target_bucket,target_prefix,parallelism,overwrite)
    val runnable  = for {
                      _   <- Task.succeed(logger.info("#"*100))
                      _   <- Task.succeed(logger.info(s"Source GCS path gs://$src_bucket/$src_prefix"))
                      _   <- Task.succeed(logger.info(s"Target GCS path gs://$target_bucket/$target_prefix"))
                      _   <- program.provideLayer(env).foldM(
                              ex => Task.succeed(logger.error(ex.getMessage)) *> Task.fail(ex),
                              _  => Task.succeed(logger.info(s"Successfully copied objects"))
                            )
                      _   <- Task.succeed(logger.info("#"*100))
                    } yield ()
    runnable
  }
}





