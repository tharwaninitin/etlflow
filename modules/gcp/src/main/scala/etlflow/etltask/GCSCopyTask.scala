package etlflow.etltask

import etlflow.gcp.Location
import gcp4zio._
import zio.{RIO, Task}

@SuppressWarnings(Array("org.wartremover.warts.Throw", "org.wartremover.warts.ToString"))
case class GCSCopyTask(
    name: String,
    input: Location,
    inputRecursive: Boolean,
    output: Location,
    parallelism: Int,
    overwrite: Boolean = true
) extends EtlTaskZIO[GCSEnv, Unit] {

  override protected def processZio: RIO[GCSEnv, Unit] = {
    val program = (input, output) match {
      case (src @ Location.GCS(_, _), tgt @ Location.GCS(_, _)) =>
        GCSApi.copyObjectsGCStoGCS(
          src.bucket,
          Some(src.path),
          inputRecursive,
          List.empty,
          tgt.bucket,
          Some(tgt.path),
          parallelism
        )
      case (src @ Location.LOCAL(_), tgt @ Location.GCS(_, _)) =>
        GCSApi.copyObjectsLOCALtoGCS(src.path, tgt.bucket, tgt.path, parallelism, overwrite)
      case (src, tgt) =>
        Task(throw new RuntimeException(s"Copying data between source $src to target $tgt is not implemented yet"))
    }
    val runnable = for {
      _ <- Task.succeed(logger.info("#" * 100))
      _ <- Task.succeed(logger.info(s"Source Filesystem $input"))
      _ <- Task.succeed(logger.info(s"Target Filesystem $output"))
      _ <- program.tapError(ex => Task.succeed(logger.error(ex.getMessage)))
      _ <- Task.succeed(logger.info(s"Successfully copied objects" + "\n" + "#" * 100))
    } yield ()
    runnable
  }

  override def getStepProperties: Map[String, String] = Map(
    "input"       -> input.toString,
    "output"      -> output.toString,
    "parallelism" -> parallelism.toString,
    "overwrite"   -> overwrite.toString
  )
}
