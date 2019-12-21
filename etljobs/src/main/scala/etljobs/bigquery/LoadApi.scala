package etljobs.bigquery

import org.apache.log4j.Logger
import com.google.cloud.bigquery.{BigQuery, FormatOptions, JobInfo, LoadJobConfiguration, StandardTableDefinition, TableId, WriteChannelConfiguration}
import scala.sys.process._

case class BQLoadException(msg : String) extends Exception
object LoadApi {
    private val load_logger = Logger.getLogger(getClass.getName)
    load_logger.info(s"Loaded ${getClass.getName}")

    def loadIntoBQFromLocalFile( 
        source_path: String
        ,source_paths_partitions: Seq[(String,String)]
        ,source_format: FormatOptions
        ,destination_dataset: String
        ,destination_table: String
        ,write_disposition: JobInfo.WriteDisposition
        ,create_disposition: JobInfo.CreateDisposition
        ) : Unit = {
        if (source_paths_partitions.nonEmpty) {
            load_logger.info(s"No of BQ partitions: ${source_paths_partitions.length}" )
            source_paths_partitions.foreach { case(src_path, partition) =>
                val table_partition = destination_table + "$" + partition
                val full_table_name = destination_dataset + "." + table_partition
                val bq_load_cmd =s"""bq load --replace  --time_partitioning_field date --require_partition_filter=false --source_format=${source_format.getType} $full_table_name $src_path""".stripMargin
                load_logger.info(s"Loading data from path: $src_path")
                load_logger.info(s"Destination table: $full_table_name")
                load_logger.info(s"BQ Load command is: $bq_load_cmd")
                val x = s"$bq_load_cmd".!
                load_logger.info(s"Output exit code: $x")
                if (x != 0) throw BQLoadException("BQ error")
            }
        }
        else {
            load_logger.info("BQ file path: " + source_path)
            val full_table_name = destination_dataset + "." + destination_table
            val bq_load_cmd =s"""bq load --replace --source_format=${source_format.getType} $full_table_name $source_path""".stripMargin
            load_logger.info(s"Loading data from path: $source_path")
            load_logger.info(s"Destination table: $full_table_name")
            load_logger.info(s"BQ Load command is: $bq_load_cmd")
            val x = s"$bq_load_cmd".!
            load_logger.info(s"Output exit code: $x")
            if (x != 0) throw BQLoadException("BQ error")
        }
    }
    
    def loadIntoPartitionedBQTableFromGCS(
        bq: BigQuery
        ,source_paths_partitions: Seq[(String,String)]
        ,source_format: FormatOptions
        ,destination_dataset: String
        ,destination_table: String
        ,write_disposition: JobInfo.WriteDisposition
        ,create_disposition: JobInfo.CreateDisposition
        ): Unit ={
        
        load_logger.info(s"No of BQ partitions: ${source_paths_partitions.length}" )
        source_paths_partitions.par.foreach { case (src_path, partition) =>
            val table_partition = destination_table + "$" + partition             
            loadIntoUnpartitionedBQTableFromGCS(bq,src_path,source_format,destination_dataset,table_partition,write_disposition,create_disposition)
        }
    }

    def loadIntoUnpartitionedBQTableFromGCS(
        bq: BigQuery
        ,source_path: String
        ,source_format: FormatOptions
        ,destination_dataset: String
        ,destination_table: String
        ,write_disposition: JobInfo.WriteDisposition
        ,create_disposition: JobInfo.CreateDisposition
        ): Unit = {

        // Create BQ table instance
        val tableId = TableId.of(destination_dataset, destination_table)
        
        // Create BQ Load configuration
        val configuration: LoadJobConfiguration = LoadJobConfiguration
            .builder(tableId, source_path)
            .setFormatOptions(source_format)
            .setWriteDisposition(write_disposition)
            .setCreateDisposition(create_disposition)
            .build()

        // Create BQ Load job
        val loadJob = bq.create(JobInfo.of(configuration))

        // Wait for the job to complete
        val completedJob = loadJob.waitFor()
        val destinationTable = bq.getTable(tableId).getDefinition[StandardTableDefinition]

        load_logger.info(s"Source path: $source_path")
        load_logger.info(s"Destination table: ${destination_dataset}.${destination_table}")
        load_logger.info(s"Job State: ${completedJob.getStatus.getState}")
        load_logger.info(s"Loaded rows: ${destinationTable.getNumRows}")
        load_logger.info(s"Loaded rows size: ${destinationTable.getNumBytes / 1000000.0} MB")
    }
}