package etlflow.audit

import com.google.cloud.bigquery.{BigQuery, JobId, JobInfo, QueryJobConfiguration}
import etlflow.log.ApplicationLogger
import etlflow.model.{JobRun, TaskRun}
import gcp4zio.bq.BQClient
import zio.{Task, TaskLayer, UIO, ZIO, ZLayer}
import java.util.UUID

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Throw", "org.wartremover.warts.ToString"))
object BQ extends ApplicationLogger {

  private[etlflow] case class BQAudit(jobRunId: String, client: BigQuery) extends etlflow.audit.Audit {
    private def executeQuery(query: String): Task[Unit] = ZIO.attempt {
      val queryConfig: QueryJobConfiguration = QueryJobConfiguration
        .newBuilder(query)
        .setUseLegacySql(false)
        .build()

      val jobId    = JobId.of(UUID.randomUUID().toString)
      var queryJob = client.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())

      // Wait for the query to complete.
      try queryJob = queryJob.waitFor()
      catch { case e: Throwable => e.printStackTrace() }

      if (queryJob == null)
        throw new RuntimeException("Job no longer exists")
      else if (queryJob.getStatus.getError != null) {
        logger.error(queryJob.getStatus.getState.toString)
        throw new RuntimeException(s"Error ${queryJob.getStatus.getError.getMessage}")
      } else {
        logger.info(s"Job State: ${queryJob.getStatus.getState}")
      }
    }

    override def logTaskStart(
        taskRunId: String,
        taskName: String,
        props: Map[String, String],
        taskType: String
    ): UIO[Unit] = executeQuery("INSERT").ignore

    override def logTaskEnd(
        taskRunId: String,
        taskName: String,
        props: Map[String, String],
        taskType: String,
        error: Option[Throwable]
    ): UIO[Unit] = executeQuery("INSERT").ignore

    override def logJobStart(jobName: String, args: Map[String, String], props: Map[String, String]): UIO[Unit] =
      executeQuery("INSERT").ignore

    override def logJobEnd(
        jobName: String,
        args: Map[String, String],
        props: Map[String, String],
        error: Option[Throwable]
    ): UIO[Unit] =
      executeQuery("INSERT").ignore

    override def getJobRuns(query: String): UIO[Iterable[JobRun]] = ???

    override def getTaskRuns(query: String): UIO[Iterable[TaskRun]] = ???
  }

  def apply(jri: String, credentials: Option[String] = None): TaskLayer[Audit] =
    ZLayer.fromZIO(BQClient(credentials).map(bq => BQAudit(jri, bq)))
}
