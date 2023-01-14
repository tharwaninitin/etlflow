package etlflow.audit

import etlflow.db.DBImpl
import etlflow.log.ApplicationLogger
import etlflow.model.Credential.JDBC
import etlflow.model.{JobRun, TaskRun}
import etlflow.utils.MapToJson
import scalikejdbc.WrappedResultSet
import zio.{Task, TaskLayer, UIO, ZIO, ZLayer}

object DB extends ApplicationLogger {
  private[etlflow] case class DBAudit(jobRunId: String, client: DBImpl) extends etlflow.audit.Audit {
    override def logTaskStart(
        taskRunId: String,
        taskName: String,
        props: Map[String, String],
        taskType: String
    ): UIO[Unit] =
      client
        .executeQuery(Sql.insertTaskRun(taskRunId, taskName, MapToJson(props), taskType, jobRunId))
        .fold(e => logger.error(e.getMessage), _ => ())

    override def logTaskEnd(
        taskRunId: String,
        taskName: String,
        props: Map[String, String],
        taskType: String,
        error: Option[Throwable]
    ): UIO[Unit] = {
      val status = error.fold("pass")(ex => s"failed with error: ${ex.getMessage}")
      client
        .executeQuery(Sql.updateTaskRun(taskRunId, MapToJson(props), status))
        .fold(e => logger.error(e.getMessage), _ => ())
    }

    override def logJobStart(jobName: String, props: Map[String, String]): UIO[Unit] = {
      val properties = MapToJson(props)
      client
        .executeQuery(Sql.insertJobRun(jobRunId, jobName, properties))
        .fold(e => logger.error(e.getMessage), _ => ())
    }

    override def logJobEnd(jobName: String, props: Map[String, String], error: Option[Throwable]): UIO[Unit] = {
      val status     = error.fold("pass")(ex => s"failed with error: ${ex.getMessage}")
      val properties = MapToJson(props)
      client
        .executeQuery(Sql.updateJobRun(jobRunId, status, properties))
        .fold(e => logger.error(e.getMessage), _ => ())
    }

    override def getJobRuns(query: String): Task[Iterable[JobRun]] = client
      .fetchResults(query) { rs =>
        JobRun(
          rs.string("job_run_id"),
          rs.string("job_name"),
          rs.string("props"),
          rs.string("status"),
          rs.zonedDateTime("created_at"),
          rs.zonedDateTime("updated_at")
        )
      }
      .tapError(ex => ZIO.logError(ex.getMessage))

    override def getTaskRuns(query: String): Task[Iterable[TaskRun]] = client
      .fetchResults(query) { rs =>
        TaskRun(
          rs.string("task_run_id"),
          rs.string("job_run_id"),
          rs.string("task_name"),
          rs.string("task_type"),
          rs.string("props"),
          rs.string("status"),
          rs.zonedDateTime("created_at"),
          rs.zonedDateTime("updated_at")
        )
      }
      .tapError(ex => ZIO.logError(ex.getMessage))

    override type RS = WrappedResultSet
    override def fetchResults(query: String): Task[Iterable[WrappedResultSet]] = client.fetchResults(query)(identity)
  }

  private[etlflow] def layer(jobRunId: String, fetchSize: Option[Int]): ZLayer[String, Throwable, Audit] =
    ZLayer(ZIO.service[String].map(pool => DBAudit(jobRunId, DBImpl(pool, fetchSize))))

  def apply(
      db: JDBC,
      jobRunId: String,
      poolName: String = "EtlFlow-Audit-Pool",
      poolSize: Int = 2,
      fetchSize: Option[Int] = None
  ): TaskLayer[Audit] =
    etlflow.db.CP.layer(db, poolName, poolSize) >>> layer(jobRunId, fetchSize)
}
