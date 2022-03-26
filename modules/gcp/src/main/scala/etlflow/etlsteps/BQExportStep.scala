package etlflow.etlsteps

import gcp4zio.{BQApi, BQEnv, BQInputType}
import zio.{RIO, UIO}

case class BQExportStep(
    name: String,
    sourceProject: Option[String] = None,
    sourceDataset: String,
    sourceTable: String,
    destinationPath: String,
    destinationFileName: Option[String] = None,
    destinationFormat: BQInputType,
    destinationCompressionType: String = "gzip"
) extends EtlStep[BQEnv, Unit] {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  var rowCount: Map[String, Long] = Map.empty

  protected def process: RIO[BQEnv, Unit] = {
    logger.info("#" * 50)
    logger.info(s"Starting BQ Data Export Step: $name")
    BQApi.exportTable(
      sourceDataset,
      sourceTable,
      sourceProject,
      destinationPath,
      destinationFileName,
      destinationFormat,
      destinationCompressionType
    ) *> UIO(logger.info("#" * 50))
  }

  override def getExecutionMetrics: Map[String, String] =
    Map(
      "total_rows" -> rowCount.foldLeft(0L)((a, b) => a + b._2).toString
    )
  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getStepProperties: Map[String, String] = Map(
    "input_project"   -> sourceProject.getOrElse(""),
    "input_dataset"   -> sourceDataset,
    "input_table"     -> sourceTable,
    "output_type"     -> destinationFormat.toString,
    "output_location" -> destinationPath
  )
}
