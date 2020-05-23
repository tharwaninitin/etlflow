package etlflow.bigquery

import java.util.UUID
import com.google.cloud.bigquery.JobStatistics.QueryStatistics
import com.google.cloud.bigquery.{BigQuery, FieldValueList, JobId, JobInfo, QueryJobConfiguration, TableResult}
import org.slf4j.LoggerFactory

object QueryApi {
  private val query_logger = LoggerFactory.getLogger(getClass.getName)
  query_logger.info(s"Loaded ${getClass.getName}")

  def getDataFromBQ(bq: BigQuery, query: String): Iterable[FieldValueList] = {
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
  def executeQuery(bq: BigQuery, query: String): Unit = {
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
      query_logger.error(queryJob.getStatus.getState.toString)
      throw new RuntimeException(s"Error ${queryJob.getStatus.getError.getMessage}")
    }
    else {
      query_logger.info(s"Job State: ${queryJob.getStatus.getState}")
      // val stats = queryJob.getStatistics.asInstanceOf[QueryStatistics]
      // query_logger.info(s"Query Plan : ${stats.getQueryPlan}")
    }
  }
}