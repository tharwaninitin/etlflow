package etljobs.bigquery

import org.apache.log4j.Logger
import java.util.UUID
import com.google.cloud.bigquery.{BigQuery, FieldValueList, Job, JobId, JobInfo, QueryJobConfiguration, TableId, TableResult}

object QueryApi {
  private val query_logger = Logger.getLogger(getClass.getName)
  query_logger.info(s"Loaded ${getClass.getName}")

  def getDataFromBQ(bq: BigQuery, query: String): Iterable[FieldValueList] = {
    val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(query)
      .setUseLegacySql(false)
      .build()

    val jobId: JobId = JobId.of(UUID.randomUUID().toString)
    val queryJob: Job = bq.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())

    // Wait for the query to complete.
    queryJob.waitFor()

    import scala.collection.JavaConverters._
    val result: TableResult = queryJob.getQueryResults()
    result.iterateAll().asScala
  }

  def loadIntoBQFromBQ(bq: BigQuery,
                       seq_query_destination_table: Seq[(String, String)],
                       destination_dataset: String,
                       destination_table: String,
                       write_disposition: JobInfo.WriteDisposition,
                       create_disposition: JobInfo.CreateDisposition
                      ): Unit = {
    seq_query_destination_table.foreach { pair_query_destination_table =>

      val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(pair_query_destination_table._1)
        .setUseLegacySql(false)
        .setDestinationTable(TableId.of(destination_dataset, pair_query_destination_table._2))
        .setWriteDisposition(write_disposition)
        .setCreateDisposition(create_disposition)
        .setAllowLargeResults(true)
        .build()

      val jobId: JobId = JobId.of(UUID.randomUUID().toString)

      val queryJob: Job = bq.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())

      queryJob.waitFor()

      query_logger.info(s"BQ table ${destination_dataset + "." + pair_query_destination_table._2} has been updated successfully")
    }
  }
}