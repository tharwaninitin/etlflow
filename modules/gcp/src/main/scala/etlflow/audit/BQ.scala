package etlflow.audit

import etlflow.log.ApplicationLogger
import etlflow.model.{JobRun, TaskRun}
import etlflow.utils.MapToJson
import gcp4zio.bq.{BQClient, BQImpl}
import zio.{TaskLayer, UIO, ZLayer}
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Throw", "org.wartremover.warts.ToString"))
object BQ extends ApplicationLogger {

  private[etlflow] case class BQAudit(jobRunId: String, client: BQImpl) extends etlflow.audit.Audit {

    override def logTaskStart(
        taskRunId: String,
        taskName: String,
        props: Map[String, String],
        taskType: String
    ): UIO[Unit] = client
      .executeQuery(Sql.insertTaskRun(taskRunId, taskName, MapToJson(props), taskType, jobRunId))
      .fold(
        e => logger.error(e.getMessage),
        op => op
      )

    override def logTaskEnd(
        taskRunId: String,
        taskName: String,
        props: Map[String, String],
        taskType: String,
        error: Option[Throwable]
    ): UIO[Unit] = client
      .executeQuery(
        Sql.updateTaskRun(taskRunId, MapToJson(props), error.fold("pass")(ex => s"failed with error: ${ex.getMessage}"))
      )
      .fold(
        e => logger.error(e.getMessage),
        op => op
      )

    override def logJobStart(jobName: String, args: Map[String, String], props: Map[String, String]): UIO[Unit] = client
      .executeQuery(Sql.insertJobRun(jobRunId, jobName, MapToJson(args), MapToJson(props)))
      .fold(
        e => logger.error(e.getMessage),
        op => op
      )

    override def logJobEnd(
        jobName: String,
        args: Map[String, String],
        props: Map[String, String],
        error: Option[Throwable]
    ): UIO[Unit] = client
      .executeQuery(
        Sql.updateJobRun(jobRunId, error.fold("pass")(ex => s"failed with error: ${ex.getMessage}"), MapToJson(props))
      )
      .fold(
        e => logger.error(e.getMessage),
        op => op
      )

    override def getJobRuns(query: String): UIO[Iterable[JobRun]] = client
      .getData(query)(fl =>
        JobRun(
          fl.get("job_run_id").getStringValue,
          fl.get("job_name").getStringValue,
          fl.get("args").getStringValue,
          fl.get("props").getStringValue,
          fl.get("status").getStringValue,
          fl.get("created_at").getTimestampInstant.atZone(ZoneId.systemDefault()),
          fl.get("updated_at").getTimestampInstant.atZone(ZoneId.systemDefault())
        )
      )
      .fold(
        { e =>
          logger.error(e.getMessage)
          List.empty
        },
        op => op
      )

    override def getTaskRuns(query: String): UIO[Iterable[TaskRun]] = client
      .getData(query)(fl =>
        TaskRun(
          fl.get("task_run_id").getStringValue,
          fl.get("job_run_id").getStringValue,
          fl.get("task_name").getStringValue,
          fl.get("task_type").getStringValue,
          fl.get("props").getStringValue,
          fl.get("status").getStringValue,
          ZonedDateTime.of(LocalDateTime.parse(fl.get("created_at").getStringValue), ZoneId.systemDefault()),
          ZonedDateTime.of(LocalDateTime.parse(fl.get("updated_at").getStringValue), ZoneId.systemDefault())
        )
      )
      .fold(
        { e =>
          logger.error(e.getMessage)
          List.empty
        },
        op => op
      )
  }

  def apply(jri: String, credentials: Option[String] = None): TaskLayer[Audit] =
    ZLayer.fromZIO(BQClient(credentials).map(bq => BQAudit(jri, BQImpl(bq))))
}
