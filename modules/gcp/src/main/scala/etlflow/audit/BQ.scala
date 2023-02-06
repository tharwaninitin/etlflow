package etlflow.audit

import com.google.cloud.bigquery.FieldValueList
import etlflow.gcp.logBQJobs
import etlflow.model.{JobRun, TaskRun}
import gcp4zio.bq.{BQClient, BQImpl}
import zio.{Task, TaskLayer, UIO, ZIO, ZLayer}
import java.time.ZoneId
import java.util.UUID

@SuppressWarnings(Array("org.wartremover.warts.ToString", "org.wartremover.warts.AsInstanceOf"))
object BQ {

  private[etlflow] case class BQAudit(jobRunId: String, client: BQImpl) extends etlflow.audit.Audit {

    override def logTaskStart(taskRunId: String, taskName: String, props: String, taskType: String): UIO[Unit] = logBQJobs(
      client.executeQuery(Sql.insertTaskRun(taskRunId, taskName, props, taskType, jobRunId))
    )

    override def logTaskEnd(taskRunId: String, error: Option[Throwable]): UIO[Unit] = logBQJobs(
      client.executeQuery(Sql.updateTaskRun(taskRunId, error.fold("pass")(ex => s"failed with error: ${ex.getMessage}")))
    )

    override def logJobStart(jobName: String, props: String): UIO[Unit] = logBQJobs(
      client.executeQuery(Sql.insertJobRun(jobRunId, jobName, props))
    )

    override def logJobEnd(error: Option[Throwable]): UIO[Unit] = logBQJobs(
      client.executeQuery(Sql.updateJobRun(jobRunId, error.fold("pass")(ex => s"failed with error: ${ex.getMessage}")))
    )

    override def getJobRuns(query: String): Task[Iterable[JobRun]] = client
      .fetchResults(query)(fl =>
        JobRun(
          fl.get("job_run_id").getStringValue,
          fl.get("job_name").getStringValue,
          fl.get("props").getStringValue,
          fl.get("status").getStringValue,
          fl.get("created_at").getTimestampInstant.atZone(ZoneId.systemDefault()),
          fl.get("updated_at").getTimestampInstant.atZone(ZoneId.systemDefault())
        )
      )
      .tapError(ex => ZIO.logError(ex.getMessage))

    override def getTaskRuns(query: String): Task[Iterable[TaskRun]] = client
      .fetchResults(query)(fl =>
        TaskRun(
          fl.get("task_run_id").getStringValue,
          fl.get("job_run_id").getStringValue,
          fl.get("task_name").getStringValue,
          fl.get("task_type").getStringValue,
          fl.get("props").getStringValue,
          fl.get("status").getStringValue,
          fl.get("created_at").getTimestampInstant.atZone(ZoneId.systemDefault()),
          fl.get("updated_at").getTimestampInstant.atZone(ZoneId.systemDefault())
        )
      )
      .tapError(ex => ZIO.logError(ex.getMessage))

    override def fetchResults[RS, T](query: String)(fn: RS => T): Task[Iterable[T]] =
      client.fetchResults(query)(fn.asInstanceOf[FieldValueList => T]).tapError(ex => ZIO.logError(ex.getMessage))

    override def executeQuery(query: String): Task[Unit] =
      client.executeQuery(query).tapError(ex => ZIO.logError(ex.getMessage)).unit
  }

  def apply(jri: String = UUID.randomUUID.toString, credentials: Option[String] = None): TaskLayer[Audit] =
    ZLayer.fromZIO(BQClient(credentials).map(bq => BQAudit(jri, BQImpl(bq))))
}
