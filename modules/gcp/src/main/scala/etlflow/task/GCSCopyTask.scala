package etlflow.task

import etlflow.gcp.Location
import gcp4zio.gcs._
import zio.{RIO, ZIO}

@SuppressWarnings(Array("org.wartremover.warts.Throw", "org.wartremover.warts.ToString"))
case class GCSCopyTask(
    name: String,
    input: Location,
    inputRecursive: Boolean,
    output: Location,
    parallelism: Int,
    overwrite: Boolean = true
) extends EtlTask[GCS, Unit] {

  override protected def process: RIO[GCS, Unit] = {
    val program = (input, output) match {
      case (src @ Location.GCS(_, _), tgt @ Location.GCS(_, _)) =>
        GCS.copyObjectsGCStoGCS(
          src.bucket,
          Some(src.path),
          inputRecursive,
          List.empty,
          tgt.bucket,
          Some(tgt.path),
          parallelism
        )
      case (src @ Location.LOCAL(_), tgt @ Location.GCS(_, _)) =>
        GCS.copyObjectsLOCALtoGCS(src.path, tgt.bucket, tgt.path, parallelism, overwrite)
      case (src, tgt) =>
        ZIO.attempt(throw new RuntimeException(s"Copying data between source $src to target $tgt is not implemented yet"))
    }
    val runnable = for {
      _ <- ZIO.succeed(logger.info("#" * 100))
      _ <- ZIO.succeed(logger.info(s"Source Filesystem $input"))
      _ <- ZIO.succeed(logger.info(s"Target Filesystem $output"))
      _ <- program.tapError(ex => ZIO.succeed(logger.error(ex.getMessage)))
      _ <- ZIO.succeed(logger.info(s"Successfully copied objects" + "\n" + "#" * 100))
    } yield ()
    runnable
  }

  override def getTaskProperties: Map[String, String] = Map(
    "input"       -> input.toString,
    "output"      -> output.toString,
    "parallelism" -> parallelism.toString,
    "overwrite"   -> overwrite.toString
  )
}
