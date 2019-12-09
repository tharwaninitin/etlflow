package etljobs.bigquery

import org.apache.log4j.Logger
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import com.google.cloud.bigquery.{BigQuery, FormatOptions, JobInfo, LoadJobConfiguration, StandardTableDefinition, TableId, WriteChannelConfiguration}
import etljobs.utils.{IOType, ORC, PARQUET}
import scala.sys.process._

object LoadApi {
    private val load_logger = Logger.getLogger(getClass.getName)
    load_logger.info(s"Loaded ${getClass.getName}")

    def loadIntoBQShell( 
        spark: SparkSession
        ,source_path: String
        ,source_dirs: => Seq[String]
        ,source_format: IOType
        ,destination_dataset: String
        ,destination_table: String
        ,write_disposition: JobInfo.WriteDisposition
        ,create_disposition: JobInfo.CreateDisposition
        ) : Unit = {
            val source_format_bq: FormatOptions = source_format match {
                case PARQUET => FormatOptions.parquet
                case ORC => FormatOptions.orc
                case _ => FormatOptions.parquet
              }
            if (source_dirs.nonEmpty) {
                load_logger.info("BQ file paths: " + source_dirs.toList)
                source_dirs.foreach { src_path =>
                val table_partition = destination_table + "$" + src_path.split("=").last
                val path = src_path + "/"
                val fs = FileSystem.get(new java.net.URI(path), spark.sparkContext.hadoopConfiguration)
                val file_name = fs.globStatus(new Path(path + "part*"))(0).getPath.getName
                val full_path = path + file_name
                val full_table_name = destination_dataset + "." + table_partition
                val bq_load_cmd =s"""bq load --replace  --time_partitioning_field date --require_partition_filter=false --source_format=${source_format_bq.getType} $full_table_name $full_path""".stripMargin
                load_logger.info(s"Loading data from path: $full_path")
                load_logger.info(s"Destination table: $full_table_name")
                load_logger.info(s"BQ Load command is: $bq_load_cmd")
                val x = s"$bq_load_cmd".!
                load_logger.info(s"Output exit code: $x")
                case class BQException(msg : String) extends Exception
                if (x != 0) throw BQException("BQ error")
                }
            }
            else {
                load_logger.info("BQ file path: " + source_path)
                val full_table_name = destination_dataset + "." + destination_table
                val bq_load_cmd =s"""bq load --replace --source_format=${source_format_bq.getType} $full_table_name $source_path""".stripMargin
                load_logger.info(s"Loading data from path: $source_path")
                load_logger.info(s"Destination table: $full_table_name")
                load_logger.info(s"BQ Load command is: $bq_load_cmd")
                val x = s"$bq_load_cmd".!
                load_logger.info(s"Output exit code: $x")
                case class BQException(msg : String) extends Exception
                if (x != 0) throw BQException("BQ error")
            }
        }
    
      def loadIntoBQFromGCS(
        bq : BigQuery
        ,source_path: String
        ,source_dirs: => Seq[String]
        ,source_format: IOType
        ,destination_dataset: String
        ,destination_table: String
        ,write_disposition: JobInfo.WriteDisposition
        ,create_disposition: JobInfo.CreateDisposition
        ): Unit ={
            val source_format_bq: FormatOptions = source_format match {
                case PARQUET => FormatOptions.parquet
                case ORC => FormatOptions.orc
                case _ => FormatOptions.parquet
              }
            
            if (source_dirs.nonEmpty) {
                load_logger.info("BQ file paths: " + source_dirs.toList)
                source_dirs.par.foreach { src_path =>
                    val table_partition = destination_table + "$" +  src_path.split("=").last
                    val path = src_path + "/*"
                    val tableId = TableId.of(destination_dataset, table_partition)
                    val configuration: LoadJobConfiguration = LoadJobConfiguration
                    .builder(tableId, path)
                    .setFormatOptions(source_format_bq)
                    .setWriteDisposition(write_disposition)
                    .setCreateDisposition(create_disposition)
                    .build()
            
                    // Create BQ Load job
                    val loadJob = bq.create(JobInfo.of(configuration))
            
                    // Wait for the job to complete
                    val completedJob = loadJob.waitFor()
                    val destinationTable = bq.getTable(tableId).getDefinition[StandardTableDefinition]
            
                    load_logger.info(s"Source path: $path")
                    load_logger.info(s"Destination table: ${destination_dataset + "." + table_partition}")
                    load_logger.info("Job State: " + completedJob.getStatus.getState)
                    load_logger.info("Loaded rows: " + destinationTable.getNumRows)
                    load_logger.info(s"Loaded rows size: ${destinationTable.getNumBytes / 1000000.0} MB")
                    load_logger.info("###########################################################################################")
                    }
                }
            else {
                load_logger.info(s"Loading data from single source path: $source_path")
                load_logger.info(s"Destination table: ${destination_dataset + "." +  destination_table}")
                val tableId = TableId.of(destination_dataset, destination_table)
                lazy val configuration : LoadJobConfiguration  = LoadJobConfiguration
                    .builder(tableId, source_path)
                    .setFormatOptions(source_format_bq)
                    .setWriteDisposition(write_disposition)
                    .setCreateDisposition(create_disposition)
                    .build()
            
                // Create BQ Load job
                val loadJob = bq.create(JobInfo.of(configuration))
            
                // Wait for the job to complete
                val completedJob = loadJob.waitFor()
                val destinationTable = bq.getTable(tableId).getDefinition[StandardTableDefinition]
            
                load_logger.info("Job State: " + completedJob.getStatus.getState)
                load_logger.info("Loaded rows: " + destinationTable.getNumRows)
                load_logger.info(s"Loaded rows size: ${destinationTable.getNumBytes / 1000000.0} MB")
                load_logger.info("#################################################################################################")
            }
        }
}