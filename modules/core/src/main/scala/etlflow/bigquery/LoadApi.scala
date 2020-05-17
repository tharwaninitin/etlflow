package etlflow.bigquery

import java.util.UUID

import org.apache.log4j.Logger
import com.google.cloud.bigquery.{BigQuery, CsvOptions, FormatOptions, Job, JobConfiguration,
  JobId, JobInfo, LoadJobConfiguration,
  QueryJobConfiguration, Schema, StandardTableDefinition, TableId
}
import etlflow.utils.{BQ, CSV, IOType, ORC, PARQUET}
import scala.sys.process._

object LoadApi {
  private val load_logger = Logger.getLogger(getClass.getName)
  load_logger.info(s"Loaded ${getClass.getName}")

  def getFormatOptions(input_type: IOType): FormatOptions = input_type match {
    case PARQUET => FormatOptions.parquet
    case ORC => FormatOptions.orc
    case CSV(field_delimiter, header_present, _, _) => CsvOptions.newBuilder()
      .setSkipLeadingRows(if (header_present) 1 else 0)
      .setFieldDelimiter(field_delimiter)
      .build()
    case _ => FormatOptions.parquet
  }

  def loadIntoBQFromLocalFile(
                       source_locations: Either[String, Seq[(String, String)]]
                       , source_format: IOType
                       , destination_dataset: String
                       , destination_table: String
                       , write_disposition: JobInfo.WriteDisposition
                       , create_disposition: JobInfo.CreateDisposition
                     ): Unit = {
    if (source_locations.isRight) {
      load_logger.info(s"No of BQ partitions: ${source_locations.right.get.length}")
      source_locations.right.get.foreach { case (src_path, partition) =>
        val table_partition = destination_table + "$" + partition
        val full_table_name = destination_dataset + "." + table_partition
        val bq_load_cmd =s"""bq load --replace  --time_partitioning_field date --require_partition_filter=false --source_format=${source_format.toString} $full_table_name $src_path""".stripMargin
        load_logger.info(s"Loading data from path: $src_path")
        load_logger.info(s"Destination table: $full_table_name")
        load_logger.info(s"BQ Load command is: $bq_load_cmd")
        val x = s"$bq_load_cmd".!
        load_logger.info(s"Output exit code: $x")
        if (x != 0) throw BQLoadException("Error executing BQ load command")
      }
    }
    else {
      load_logger.info("BQ file path: " + source_locations.left.get)
      val full_table_name = destination_dataset + "." + destination_table
      val bq_load_cmd =s"""bq load --replace --source_format=${source_format.toString} $full_table_name ${source_locations.left.get}""".stripMargin
      load_logger.info(s"Loading data from path: ${source_locations.left.get}")
      load_logger.info(s"Destination table: $full_table_name")
      load_logger.info(s"BQ Load command is: $bq_load_cmd")
      val x = s"$bq_load_cmd".!
      load_logger.info(s"Output exit code: $x")
      if (x != 0) throw BQLoadException("Error executing BQ load command")
    }
  }

  def loadIntoPartitionedBQTable(
                       bq: BigQuery
                       , source_paths_partitions: Seq[(String, String)]
                       , source_format: IOType
                       , destination_dataset: String
                       , destination_table: String
                       , write_disposition: JobInfo.WriteDisposition
                       , create_disposition: JobInfo.CreateDisposition
                       , schema: Option[Schema] = None
                     ): Map[String, Long] = {
    load_logger.info(s"No of BQ partitions: ${source_paths_partitions.length}")
    source_paths_partitions.par.flatMap { case (src_path, partition) =>
      val table_partition = destination_table + "$" + partition
      loadIntoBQTable(
        bq, src_path, source_format, destination_dataset
        , table_partition, write_disposition, create_disposition)
    }.toList.toMap
  }

  def loadIntoBQTable(
                       bq: BigQuery
                       , source_path: String
                       , source_format: IOType
                       , destination_dataset: String
                       , destination_table: String
                       , write_disposition: JobInfo.WriteDisposition
                       , create_disposition: JobInfo.CreateDisposition
                       , schema: Option[Schema] = None
                     ): Map[String, Long] = {
    // Create Output BQ table instance
    val tableId = TableId.of(destination_dataset, destination_table)

    val jobConfiguration: JobConfiguration = source_format match {
      case BQ => QueryJobConfiguration.newBuilder(source_path)
        .setUseLegacySql(false)
        .setDestinationTable(tableId)
        .setWriteDisposition(write_disposition)
        .setCreateDisposition(create_disposition)
        .setAllowLargeResults(true)
        .build()
      case ORC | PARQUET | CSV(_,_,_,_) => schema match {
        case Some(s) => LoadJobConfiguration
          .builder(tableId, source_path)
          .setFormatOptions(getFormatOptions(source_format))
          .setSchema(s)
          .setWriteDisposition(write_disposition)
          .setCreateDisposition(create_disposition)
          .build()
        case None => LoadJobConfiguration
          .builder(tableId, source_path)
          .setFormatOptions(getFormatOptions(source_format))
          .setWriteDisposition(write_disposition)
          .setCreateDisposition(create_disposition)
          .build()
      }
      case _ => throw BQLoadException("Unsupported Input Type")
    }

    // Create BQ job
    val jobId: JobId = JobId.of(UUID.randomUUID().toString)
    val job: Job = bq.create(JobInfo.newBuilder(jobConfiguration).setJobId(jobId).build())

    // Wait for the job to complete
    val completedJob = job.waitFor()
    val destinationTable = bq.getTable(tableId).getDefinition[StandardTableDefinition]

    if (completedJob.getStatus.getError == null) {
      load_logger.info(s"Source path: $source_path")
      load_logger.info(s"Destination table: $destination_dataset.$destination_table")
      load_logger.info(s"Job State: ${completedJob.getStatus.getState}")
      load_logger.info(s"Loaded rows: ${destinationTable.getNumRows}")
      load_logger.info(s"Loaded rows size: ${destinationTable.getNumBytes / 1000000.0} MB")
    }
    else {
      throw BQLoadException(
        s"""Could not load data in ${source_format.toString} format in table ${destination_dataset + "." + destination_table} due to error ${completedJob.getStatus.getError.getMessage}""".stripMargin)
    }
    Map(destination_table -> destinationTable.getNumRows)
  }
}