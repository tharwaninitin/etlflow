package etlflow.task

import gcp4zio.bq.{BQApi, BQEnv, BQInputType}
import zio.{RIO, ZIO}

case class BQExportTask(
    name: String,
    sourceProject: Option[String] = None,
    sourceDataset: String,
    sourceTable: String,
    destinationPath: String,
    destinationFileName: Option[String] = None,
    destinationFormat: BQInputType,
    destinationCompressionType: String = "gzip"
) extends EtlTask[BQEnv, Unit] {

  override protected def process: RIO[BQEnv, Unit] = {
    logger.info("#" * 50)
    logger.info(s"Starting BQ Data Export Task: $name")
    BQApi.exportTable(
      sourceDataset,
      sourceTable,
      sourceProject,
      destinationPath,
      destinationFileName,
      destinationFormat,
      destinationCompressionType
    ) *> ZIO.succeed(logger.info("#" * 50))
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getTaskProperties: Map[String, String] = Map(
    "input_project"   -> sourceProject.getOrElse(""),
    "input_dataset"   -> sourceDataset,
    "input_table"     -> sourceTable,
    "output_type"     -> destinationFormat.toString,
    "output_location" -> destinationPath
  )
}
