package etljobs.etlsteps

import com.google.cloud.bigquery._
import etljobs.bigquery.QueryApi
import com.google.cloud.bigquery.JobInfo.CreateDisposition.CREATE_NEVER
import com.google.cloud.bigquery.JobInfo.WriteDisposition.WRITE_TRUNCATE
import scala.util.Try

class BQQueryStep(
                   val name: String
                   ,source_pair_query_destination_table: (String,String) = ("","")
                   ,source_seq_pair_query_destination_table: Seq[(String,String)] = Seq()
                   ,source_format : FormatOptions = FormatOptions.bigtable()
                   ,destination_dataset: String
                   ,destination_table: String
                   ,write_disposition: JobInfo.WriteDisposition = JobInfo.WriteDisposition.WRITE_TRUNCATE
                   ,create_disposition: JobInfo.CreateDisposition = JobInfo.CreateDisposition.CREATE_NEVER
                 )(bq : => BigQuery)
  extends EtlStep[Unit,Unit]
{
  def process(input_state : Unit): Try[Unit] = {
    Try{
      etl_logger.info("#################################################################################################")
      etl_logger.info(s"Starting BQ Query Query Step : $name")
      QueryApi.loadIntoBQFromBQ(bq, source_seq_pair_query_destination_table ,destination_dataset,destination_table,write_disposition,create_disposition)
      etl_logger.info("#################################################################################################")
    }
  }

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

  override def getStepProperties(level: String) : Map[String,String] = {
    val tableId = TableId.of(destination_dataset, destination_table)
    val destinationTable = bq.getTable(tableId).getDefinition[StandardTableDefinition]
    if (level.equalsIgnoreCase("info"))
    {
      Map(
        "destination_dataset" -> destination_dataset
        ,"destination_table" -> destination_table
        ,"Total number of Rows" -> destinationTable.getNumRows.toString
        ,"Source Format" -> source_format.toString
      )
    }else
    {
      Map(
        "source_dirs" -> source_seq_pair_query_destination_table.mkString(",")
        ,"source_path" -> source_seq_pair_query_destination_table.mkString(",")
        ,"destination_dataset" -> destination_dataset
        ,"destination_table" -> destination_table
        ,"write_disposition" -> write_disposition.toString
        ,"create_disposition" -> create_disposition.toString
        ,"Total number of Rows" -> destinationTable.getNumRows.toString
      )
    }
  }
}
object BQQueryStep {
  def apply( name : String
             ,source_pair_query_destination_table:(String,String) = ("","")
             ,source_seq_pair_query_destination_table: Seq[(String,String)] = Seq()
             ,source_format : FormatOptions = FormatOptions.bigtable()
             ,destination_dataset: String
             ,destination_table: String
             ,write_disposition: JobInfo.WriteDisposition = WRITE_TRUNCATE
             ,create_disposition: JobInfo.CreateDisposition = CREATE_NEVER
           )(bq: => BigQuery): BQQueryStep = {
    new BQQueryStep(name, source_pair_query_destination_table, source_seq_pair_query_destination_table, source_format, destination_dataset, destination_table, write_disposition, create_disposition)(bq)
  }
}