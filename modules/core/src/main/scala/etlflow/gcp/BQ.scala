package etlflow.gcp

import java.io.FileInputStream
import java.util.UUID
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, CsvOptions, FieldValueList, FormatOptions, Job, JobConfiguration, JobId, JobInfo, LoadJobConfiguration, QueryJobConfiguration, Schema, StandardTableDefinition, TableId, TableResult}
import etlflow.utils
import etlflow.utils.{CSV, GCP, IOType, ORC, PARQUET}
import zio.{IO, Layer, Managed, Task, ZIO, ZLayer}
import scala.sys.process._

object BQ {
  case class BQLoadException(msg : String) extends RuntimeException(msg)

  private def getBQ(path: String): BigQuery = {
    val credentials: GoogleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(path))
    BigQueryOptions.newBuilder().setCredentials(credentials).build().getService
  }
  private def getFormatOptions(input_type: IOType): FormatOptions = input_type match {
    case PARQUET => FormatOptions.parquet
    case ORC => FormatOptions.orc
    case CSV(field_delimiter, header_present, _, _) => CsvOptions.newBuilder()
      .setSkipLeadingRows(if (header_present) 1 else 0)
      .setFieldDelimiter(field_delimiter)
      .build()
    case _ => FormatOptions.parquet
  }

  def live(credentials: Option[GCP] = None): Layer[Throwable, BQService] = ZLayer.fromManaged {
    val acquire = IO.effect{
      val env_path: String = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", "NOT_SET_IN_ENV")
      credentials match {
        case Some(creds) =>
          gcp_logger.info("Using GCP credentials from values passed in function")
          getBQ(creds.service_account_key_path)
        case None =>
          if (env_path == "NOT_SET_IN_ENV") {
            gcp_logger.info("Using GCP credentials from local sdk")
            BigQueryOptions.getDefaultInstance.getService
          }
          else {
            gcp_logger.info("Using GCP credentials from environment variable GOOGLE_APPLICATION_CREDENTIALS")
            getBQ(env_path)
          }
      }
    }
    Managed.fromEffect(acquire).map { bq =>
      new BQService.Service {

        def executeQuery(query: String): Task[Unit] = Task {
          val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(query)
            .setUseLegacySql(false)
            .build()

          val jobId = JobId.of(UUID.randomUUID().toString)
          var queryJob = bq.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())

          // Wait for the query to complete.
          try queryJob = queryJob.waitFor()
          catch {
            case e: InterruptedException =>
              e.printStackTrace()
          }

          if (queryJob == null)
            throw new RuntimeException("Job no longer exists")
          else if (queryJob.getStatus.getError != null) {
            gcp_logger.error(queryJob.getStatus.getState.toString)
            throw new RuntimeException(s"Error ${queryJob.getStatus.getError.getMessage}")
          }
          else {
            gcp_logger.info(s"Job State: ${queryJob.getStatus.getState}")
            // val stats = queryJob.getStatistics.asInstanceOf[QueryStatistics]
            // query_logger.info(s"Query Plan : ${stats.getQueryPlan}")
          }
        }

        def getDataFromBQ(query: String): Task[Iterable[FieldValueList]] = Task {
          val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(query)
            .setUseLegacySql(false)
            .build()

          val jobId = JobId.of(UUID.randomUUID().toString)
          val queryJob = bq.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())

          // Wait for the query to complete.
          queryJob.waitFor()

          import scala.collection.JavaConverters._
          val result: TableResult = queryJob.getQueryResults()
          result.iterateAll().asScala
        }

        def loadIntoBQFromLocalFile(
             source_locations: Either[String, Seq[(String, String)]], source_format: IOType, destination_dataset: String,
             destination_table: String, write_disposition: JobInfo.WriteDisposition, create_disposition: JobInfo.CreateDisposition
           ): Task[Unit] = Task {
          if (source_locations.isRight) {
            gcp_logger.info(s"No of BQ partitions: ${source_locations.right.get.length}")
            source_locations.right.get.foreach { case (src_path, partition) =>
              val table_partition = destination_table + "$" + partition
              val full_table_name = destination_dataset + "." + table_partition
              val bq_load_cmd =s"""bq load --replace  --time_partitioning_field date --require_partition_filter=false --source_format=${source_format.toString} $full_table_name $src_path""".stripMargin
              gcp_logger.info(s"Loading data from path: $src_path")
              gcp_logger.info(s"Destination table: $full_table_name")
              gcp_logger.info(s"BQ Load command is: $bq_load_cmd")
              val x = s"$bq_load_cmd".!
              gcp_logger.info(s"Output exit code: $x")
              if (x != 0) throw BQLoadException("Error executing BQ load command")
            }
          }
          else {
            gcp_logger.info("BQ file path: " + source_locations.left.get)
            val full_table_name = destination_dataset + "." + destination_table
            val bq_load_cmd =s"""bq load --replace --source_format=${source_format.toString} $full_table_name ${source_locations.left.get}""".stripMargin
            gcp_logger.info(s"Loading data from path: ${source_locations.left.get}")
            gcp_logger.info(s"Destination table: $full_table_name")
            gcp_logger.info(s"BQ Load command is: $bq_load_cmd")
            val x = s"$bq_load_cmd".!
            gcp_logger.info(s"Output exit code: $x")
            if (x != 0) throw BQLoadException("Error executing BQ load command")
          }
        }

        override def loadIntoPartitionedBQTable(source_paths_partitions: Seq[(String, String)],
            source_format: utils.IOType, destination_dataset: String, destination_table: String,
            write_disposition: JobInfo.WriteDisposition, create_disposition: JobInfo.CreateDisposition,
            schema: Option[Schema], parallelism: Int): Task[Map[String, Long]] = {
          gcp_logger.info(s"No of BQ partitions: ${source_paths_partitions.length}")
          ZIO.foreachParN(parallelism)(source_paths_partitions){ case (src_path, partition) =>
              val table_partition = destination_table + "$" + partition
              loadIntoBQTable(src_path, source_format, destination_dataset, table_partition, write_disposition, create_disposition)
          }.map(x => x.flatten.toMap)
        }

        override def loadIntoBQTable(source_path: String, source_format: utils.IOType, destination_dataset: String,
             destination_table: String, write_disposition: JobInfo.WriteDisposition,
             create_disposition: JobInfo.CreateDisposition, schema: Option[Schema]): Task[Map[String, Long]] = Task {
          // Create Output BQ table instance
          val tableId = TableId.of(destination_dataset, destination_table)

          val jobConfiguration: JobConfiguration = source_format match {
            case utils.BQ => QueryJobConfiguration.newBuilder(source_path)
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
            gcp_logger.info(s"Source path: $source_path")
            gcp_logger.info(s"Destination table: $destination_dataset.$destination_table")
            gcp_logger.info(s"Job State: ${completedJob.getStatus.getState}")
            gcp_logger.info(s"Loaded rows: ${destinationTable.getNumRows}")
            gcp_logger.info(s"Loaded rows size: ${destinationTable.getNumBytes / 1000000.0} MB")
          }
          else {
            throw BQLoadException(
              s"""Could not load data in ${source_format.toString} format in table ${destination_dataset + "." + destination_table} due to error ${completedJob.getStatus.getError.getMessage}""".stripMargin)
          }
          Map(destination_table -> destinationTable.getNumRows)
        }
      }
    }
  }
}
