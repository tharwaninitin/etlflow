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
            )(bq : => BigQuery)
  extends EtlStep[Unit,Unit]
{
  var row_count: Map[String, Long] = Map.empty
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

  override def getExecutionMetrics: Map[String, Map[String, String]] = {
    val tableId = TableId.of(destination_dataset, destination_table)
    val destinationTable = bq.getTable(tableId).getDefinition[StandardTableDefinition]
    Map(name ->
      Map(
        "Total number of Rows" -> destinationTable.getNumRows.toString,
        "Total size in MB" -> f"${destinationTable.getNumBytes / 1000000.0} MB"
      )
    )
  }

  override def getStepProperties(level: String) : Map[String,String] = {
    if (level.equalsIgnoreCase("info"))
    {
      Map(
        "source_format" -> source_format.toString
        ,"source_path" -> source_path
        ,"source_dirs_count" -> source_paths_partitions.length.toString
        ,"destination_dataset" -> destination_dataset
        ,"destination_table" -> destination_table
        ,"write_disposition" -> write_disposition.toString
        ,"create_disposition" -> create_disposition.toString
      ) ++ row_count.map(x => (x._1, x._2.toString))
    } else
    {
      Map(
        "source_format" -> source_format.toString
        , "source_dirs" -> source_paths_partitions.mkString(",")
        , "source_path" -> source_path
        , "destination_dataset" -> destination_dataset
        , "destination_table" -> destination_table
        , "write_disposition" -> write_disposition.toString
        , "create_disposition" -> create_disposition.toString
      ) ++ row_count.map(x => (x._1, x._2.toString))
    }
  }
}

object BQLoadStep {
  def apply( name : String
           ,source_path: => String = ""
           ,source_paths_partitions: => Seq[(String,String)] = Seq()
           ,source_format: IOType
           ,source_file_system: FSType = GCS
           ,destination_dataset: String
           ,destination_table: String
           ,write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE
           ,create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER
           )(bq: => BigQuery): BQLoadStep = {
    new BQLoadStep(name, source_path, source_paths_partitions, source_format, source_file_system, destination_dataset, destination_table, write_disposition, create_disposition)(bq)
  }
}