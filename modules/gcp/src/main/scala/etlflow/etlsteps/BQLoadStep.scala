package etlflow.etlsteps

import com.google.cloud.bigquery.{JobInfo, Schema}
import etlflow.gcp._
import etlflow.model.Credential
import zio.{Task, UIO}

class BQLoadStep private[etlflow] (
    val name: String,
    input_location: => Either[String, Seq[(String, String)]],
    input_type: BQInputType,
    input_file_system: FSType = FSType.GCS,
    output_project: Option[String] = None,
    output_dataset: String,
    output_table: String,
    output_write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE,
    output_create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER,
    credentials: Option[Credential.GCP] = None,
    schema: Option[Schema] = None
) extends EtlStep[Unit] {
  var row_count: Map[String, Long] = Map.empty

  final def process: Task[Unit] = {
    logger.info("#" * 50)
    logger.info(s"Starting BQ Data Load Step : $name")

    val env = BQ.live(credentials)

    val program: Task[Unit] = input_file_system match {
      case FSType.LOCAL =>
        logger.info(s"FileSystem: $input_file_system")
        BQApi
          .loadIntoBQFromLocalFile(
            input_location,
            input_type,
            output_dataset,
            output_table,
            output_write_disposition,
            output_create_disposition
          )
          .provideLayer(env)
      case FSType.GCS =>
        input_location match {
          case Left(value) =>
            logger.info(s"FileSystem: $input_file_system")
            BQApi
              .loadIntoBQTable(
                value,
                input_type,
                output_project,
                output_dataset,
                output_table,
                output_write_disposition,
                output_create_disposition,
                schema
              )
              .provideLayer(env)
              .map { x =>
                row_count = x
              }
          case Right(value) =>
            logger.info(s"FileSystem: $input_file_system")
            BQApi
              .loadIntoPartitionedBQTable(
                value,
                input_type,
                output_project,
                output_dataset,
                output_table,
                output_write_disposition,
                output_create_disposition,
                schema,
                10
              )
              .provideLayer(env)
              .map { x =>
                row_count = x
              }
        }
    }
    program *> UIO(logger.info("#" * 50))
  }

  override def getExecutionMetrics: Map[String, String] =
    Map(
      "total_rows" -> row_count.foldLeft(0L)((a, b) => a + b._2).toString
      // "total_size" -> destinationTable.map(x => s"${x.getNumBytes / 1000000.0} MB").getOrElse("error in getting size")
    )

  override def getStepProperties: Map[String, String] =
    Map(
      "input_type" -> input_type.toString,
      "input_location" -> input_location.fold(
        source_path => source_path,
        source_paths_partitions => source_paths_partitions.mkString(",")
      ),
      "output_dataset"                  -> output_dataset,
      "output_table"                    -> output_table,
      "output_table_write_disposition"  -> output_write_disposition.toString,
      "output_table_create_disposition" -> output_create_disposition.toString
      // ,"output_rows" -> row_count.foldLeft(0L)((a, b) => a + b._2).toString
      ,
      "output_rows" -> row_count.map(x => x._1 + "<==>" + x._2.toString).mkString(",")
    )
}

object BQLoadStep {
  def apply(
      name: String,
      input_location: => Either[String, Seq[(String, String)]],
      input_type: BQInputType,
      input_file_system: FSType = FSType.GCS,
      output_project: Option[String] = None,
      output_dataset: String,
      output_table: String,
      output_write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE,
      output_create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER,
      credentials: Option[Credential.GCP] = None,
      schema: Option[Schema] = None
  ): BQLoadStep =
    new BQLoadStep(
      name,
      input_location,
      input_type,
      input_file_system,
      output_project,
      output_dataset,
      output_table,
      output_write_disposition,
      output_create_disposition,
      credentials,
      schema
    )
}
