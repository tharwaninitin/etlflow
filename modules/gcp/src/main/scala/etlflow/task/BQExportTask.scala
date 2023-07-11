package etlflow.task

import gcp4zio.bq.{BQ, FileType}
import zio.{RIO, ZIO}

case class BQExportTask(
    name: String,
    sourceProject: Option[String] = None,
    sourceDataset: String,
    sourceTable: String,
    destinationPath: String,
    destinationFormat: FileType,
    destinationFileName: Option[String] = None,
    destinationCompressionType: String = "gzip"
) extends EtlTask[BQ, Unit] {

  override protected def process: RIO[BQ, Unit] = {
    logger.info("#" * 50)
    logger.info(s"Starting BQ Data Export Task: $name")
    BQ.exportTable(
      sourceDataset,
      sourceTable,
      sourceProject,
      destinationPath,
      destinationFormat,
      destinationFileName,
      destinationCompressionType
    ) *> ZIO.succeed(logger.info("#" * 50))
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getMetaData: Map[String, String] = Map(
    "input_project"   -> sourceProject.getOrElse(""),
    "input_dataset"   -> sourceDataset,
    "input_table"     -> sourceTable,
    "output_type"     -> destinationFormat.toString,
    "output_location" -> destinationPath
  )
}
