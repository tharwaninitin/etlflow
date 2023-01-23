package etlflow.task

import etlflow.gcp.Location
import gcp4zio.gcs._
import zio.config._
import ConfigDescriptor._
import zio.{RIO, ZIO}

@SuppressWarnings(Array("org.wartremover.warts.Throw", "org.wartremover.warts.ToString"))
case class GCSCopyTask(
    name: String,
    input: Location,
    inputRecursive: Boolean,
    output: Location,
    parallelism: Int,
    overwrite: Option[Boolean] = None
) extends EtlTask[GCS, Long] {

  override def getTaskProperties: Map[String, String] = Map(
    "input"       -> input.toString,
    "output"      -> output.toString,
    "parallelism" -> parallelism.toString,
    "overwrite"   -> overwrite.getOrElse(true).toString
  )

  override protected def process: RIO[GCS, Long] = {
    val program = (input, output) match {
      case (Location.GCS(srcBucket, srcPath), Location.GCS(tgtBucket, tgtPath)) =>
        GCS.copyObjectsGCStoGCS(
          srcBucket,
          Some(srcPath),
          inputRecursive,
          List.empty,
          tgtBucket,
          Some(tgtPath),
          parallelism
        )
      case (Location.LOCAL(localPath), Location.GCS(bucket, path)) =>
        GCS.copyObjectsLOCALtoGCS(localPath, bucket, path, parallelism, overwrite.getOrElse(true))
      case (src, tgt) =>
        ZIO.attempt(throw new RuntimeException(s"Copying data between source $src to target $tgt is not implemented yet"))
    }
    val runnable = for {
      _     <- ZIO.succeed(logger.info("#" * 100))
      _     <- ZIO.succeed(logger.info(s"Source Filesystem $input"))
      _     <- ZIO.succeed(logger.info(s"Target Filesystem $output"))
      count <- program.tapError(ex => ZIO.succeed(logger.error(ex.getMessage)))
      _     <- ZIO.succeed(logger.info(s"Successfully copied objects" + "\n" + "#" * 100))
    } yield count
    runnable
  }
}

object GCSCopyTask {

  private lazy val localConfig: ConfigDescriptor[Location.LOCAL] = string("path").to[Location.LOCAL]
  private lazy val gcsConfig: ConfigDescriptor[Location.GCS]     = string("bucket").zip(string("path")).to[Location.GCS]
  private lazy val locationConfig: ConfigDescriptor[Location] =
    enumeration(nested("local")(localConfig), nested("gcs")(gcsConfig))
  val config: ConfigDescriptor[GCSCopyTask] =
    string("name")
      .zip(nested("input")(locationConfig))
      .zip(boolean("inputRecursive"))
      .zip(nested("output")(locationConfig))
      .zip(int("parallelism"))
      .zip(boolean("overwrite").optional)
      .to[GCSCopyTask]
}
