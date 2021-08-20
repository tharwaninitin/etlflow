package etlflow.etlsteps

import etlflow.gcp._
import etlflow.schema.{Credential, LoggingLevel}
import zio.{Task, UIO}

class BQExportStep private[etlflow](
     val name: String
     , source_project: Option[String] = None
     , source_dataset: String
     , source_table: String
     , destination_path: String
     , destination_file_name:Option[String] = None
     , destination_format:BQInputType
     , destination_compression_type:String = "gzip"
     , credentials: Option[Credential.GCP] = None
     )
  extends EtlStep[Unit, Unit] {
  var row_count: Map[String, Long] = Map.empty

  final def process(input: => Unit): Task[Unit] = {
    logger.info("#" * 50)
    logger.info(s"Starting BQ Data Export Step : $name")

    val env = BQ.live(credentials)

    val program: Task[Unit] = {
      BQService.exportFromBQTable(
        source_project,source_dataset,
        source_table,destination_path,destination_file_name,destination_format,destination_compression_type
      ).provideLayer(env)
    }
    program *> UIO(logger.info("#" * 50))
  }

  override def getExecutionMetrics: Map[String, Map[String, String]] = {
    Map(name ->
      Map(
        "total_rows" -> row_count.foldLeft(0L)((a, b) => a + b._2).toString
      )
    )
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] = {
    if (level == LoggingLevel.INFO) {
      Map(
         "input_project" -> source_project.getOrElse("")
        , "input_dataset" -> source_dataset
        , "input_table" -> source_table
        , "output_type" -> destination_format.toString()
        , "output_location" -> destination_path
      )
    } else {
      Map(
         "input_project" -> source_project.getOrElse("")
        , "input_dataset" -> source_dataset
        , "input_table" -> source_table
        , "output_type" -> destination_format.toString()
        , "output_location" -> destination_path
      )
    }
  }
}

object BQExportStep {
  def apply(name: String
   , source_project: Option[String] = None
   , source_dataset: String
   , source_table: String
   , destination_path: String
   , destination_file_name:Option[String] = None
   , destination_format:BQInputType
   , destination_compression_type:String = "gzip"
   , credentials: Option[Credential.GCP] = None
  ): BQExportStep = {
    new BQExportStep(name, source_project, source_dataset, source_table, destination_path,destination_file_name
      ,destination_format,destination_compression_type, credentials)
  }
}

