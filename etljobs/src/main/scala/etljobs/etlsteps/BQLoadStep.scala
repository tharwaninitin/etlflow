package etljobs.etlsteps

import etljobs.utils.{IOType, ORC, PARQUET, FSType, LOCAL, GCS}
import org.apache.spark.sql.SparkSession
import com.google.cloud.bigquery.{BigQuery, FormatOptions, JobInfo, StandardTableDefinition, TableId}
import etljobs.bigquery.LoadApi
import scala.util.{Failure, Success, Try}

class BQLoadStep(
            val name: String
            ,source_path: => String = ""
            ,source_paths_partitions: => Seq[(String,String)] = Seq()
            ,source_format: IOType
            ,source_file_system: FSType = GCS
            ,destination_dataset: String
            ,destination_table: String
            ,write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE
            ,create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER
            )(bq : => BigQuery, etl_metadata : Map[String, String])
  extends EtlStep[Unit,Unit]
{
  def process(input_state : Unit): Try[Unit] = {
    Try{
      etl_logger.info("#################################################################################################")
      etl_logger.info(s"Starting BQ Data Load Step : $name")

      val source_format_bq: FormatOptions = source_format match {
        case PARQUET => FormatOptions.parquet
        case ORC => FormatOptions.orc
        case _ => FormatOptions.parquet
      }

      if (source_file_system == LOCAL) {
        etl_logger.info(s"FileSystem: $source_file_system")
        LoadApi.loadIntoBQFromLocalFile(
          source_path,source_paths_partitions,source_format_bq
          ,destination_dataset,destination_table,write_disposition,create_disposition
        )
      }
      else if (source_paths_partitions.nonEmpty && source_file_system == GCS) {
        etl_logger.info(s"FileSystem: $source_file_system")
        LoadApi.loadIntoPartitionedBQTableFromGCS(
          bq,source_paths_partitions,source_format_bq
          ,destination_dataset,destination_table,write_disposition,create_disposition
        )
      }
      else if (source_path != "" && source_file_system == GCS) {
        etl_logger.info(s"FileSystem: $source_file_system")
        LoadApi.loadIntoUnpartitionedBQTableFromGCS(
          bq,source_path,source_format_bq
          ,destination_dataset,destination_table,write_disposition,create_disposition
        )
      }
      etl_logger.info("#################################################################################################")
    }
  }

  // def map(func: Unit => Unit): Unit = {
  //   etl_logger.info("Executing inside map")
  //   val output = Try(process()) match {
  //     case Success(value) => value
  //     case Failure(exception) => throw exception
  //   }
  //   func(output)
  // }

  // def flatMap(func: Unit => Unit): Unit = {
  //   etl_logger.info("Executing inside flatMap")
  //   val output = Try(process()) match {
  //     case Success(value) => value
  //     case Failure(exception) =>
  //       etl_logger.info(s"Got Error: $exception")
  //       throw exception
  //   }
  //   func(output)
  // }

  override def getExecutionMetrics : Map[String, Map[String,String]] = {
    val tableId = TableId.of(destination_dataset, destination_table)
    val destinationTable = bq.getTable(tableId).getDefinition[StandardTableDefinition]
    Map(name ->
      Map(
        "Total number of Rows" -> destinationTable.getNumRows.toString,
        "Total size in MB" -> f"${destinationTable.getNumBytes / 1000000.0} MB"
      )
    )
  }

  override def getStepProperties : Map[String,String] = {
    Map(
      "source_dirs" -> source_paths_partitions.mkString(",")
      ,"source_path" -> source_path
      ,"destination_dataset" -> destination_dataset
      ,"destination_table" -> destination_table
      ,"write_disposition" -> write_disposition.toString
      ,"create_disposition" -> create_disposition.toString
    )
  }
}

object BQLoadStep {
  def apply( name : String
           ,source_path: String = ""
           ,source_paths_partitions: => Seq[(String,String)] = Seq()
           ,source_format: IOType
           ,source_file_system: FSType = GCS
           ,destination_dataset: String
           ,destination_table: String
           ,write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE
           ,create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER
           )(bq: => BigQuery,etl_metadata : Map[String, String]): BQLoadStep = {
    new BQLoadStep(name, source_path, source_paths_partitions, source_format, source_file_system, destination_dataset, destination_table, write_disposition, create_disposition)(bq,etl_metadata)
  }
}