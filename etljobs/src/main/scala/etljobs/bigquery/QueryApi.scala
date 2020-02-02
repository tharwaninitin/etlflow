package etljobs.bigquery

import org.apache.log4j.Logger
import java.util.UUID
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, FieldValueList, Job, JobId, JobInfo, QueryJobConfiguration, TableResult}

object QueryApi {
    private val query_logger = Logger.getLogger(getClass.getName)
    query_logger.info(s"Loaded ${getClass.getName}")
    
    def getDataFromBQ(bq: BigQuery, query: String): Iterable[FieldValueList] = {
        val queryConfig: QueryJobConfiguration = QueryJobConfiguration.newBuilder(query)
            .setUseLegacySql(false)
            .build()

        val jobId: JobId = JobId.of(UUID.randomUUID().toString())
        val queryJob: Job = bq.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())

        // Wait for the query to complete.
        queryJob.waitFor()

        import scala.collection.JavaConverters._
        val result: TableResult = queryJob.getQueryResults()
        result.iterateAll().asScala
    }
}