package etlflow.audit

import etlflow.db.DBImpl
import etlflow.log.ApplicationLogger
import etlflow.model.Credential.JDBC
import etlflow.model.{JobRun, TaskRun}
import scalikejdbc.WrappedResultSet
import zio._
import java.util.UUID

@SuppressWarnings(Array("org.wartremover.warts.ToString", "org.wartremover.warts.AsInstanceOf"))
object DB extends ApplicationLogger {

  private[etlflow] case class DBAudit(jobRunId: String, logStackTrace: Boolean, client: DBImpl) extends etlflow.audit.Audit {

    def log[R](effect: RIO[R, Unit]): URIO[R, Unit] =
      if (logStackTrace)
        effect.fold(e => e.getStackTrace.foreach(st => logger.error(st.toString)), _ => ())
      else
        effect.fold(_ => (), _ => ())

    override def logTaskStart(taskRunId: String, taskName: String, metadata: String, taskType: String): UIO[Unit] = log(
      client
        .executeQuery(Sql.insertTaskRun(taskRunId, taskName, metadata, taskType, jobRunId, "started"))
    )

    override def logTaskEnd(taskRunId: String, error: Option[Throwable]): UIO[Unit] = log(
      client
        .executeQuery(Sql.updateTaskRun(taskRunId, error.fold("success")(ex => s"failed with error: ${ex.getMessage}")))
    )

    override def logJobStart(jobName: String, metadata: String): UIO[Unit] = log(
      client
        .executeQuery(Sql.insertJobRun(jobRunId, jobName, metadata, "started"))
    )

    override def logJobEnd(error: Option[Throwable]): UIO[Unit] = log(
      client
        .executeQuery(Sql.updateJobRun(jobRunId, error.fold("success")(ex => s"failed with error: ${ex.getMessage}")))
    )

    override def getJobRuns(query: String): Task[Iterable[JobRun]] = client
      .fetchResults(query) { rs =>
        JobRun(
          rs.string("job_run_id"),
          rs.string("job_name"),
          rs.string("metadata"),
          rs.string("status"),
          rs.zonedDateTime("created_at"),
          rs.zonedDateTime("updated_at")
        )
      }

    override def getTaskRuns(query: String): Task[Iterable[TaskRun]] = client
      .fetchResults(query) { rs =>
        TaskRun(
          rs.string("task_run_id"),
          rs.string("job_run_id"),
          rs.string("task_name"),
          rs.string("task_type"),
          rs.string("metadata"),
          rs.string("status"),
          rs.zonedDateTime("created_at"),
          rs.zonedDateTime("updated_at")
        )
      }

    override def fetchResults[RS, T](query: String)(fn: RS => T): Task[Iterable[T]] =
      client.fetchResults(query)(fn.asInstanceOf[WrappedResultSet => T])

    override def executeQuery(query: String): Task[Unit] = client.executeQuery(query)
  }

  private[etlflow] def layer(jobRunId: String, fetchSize: Option[Int], logStackTrace: Boolean): ZLayer[String, Throwable, Audit] =
    ZLayer(ZIO.serviceWith[String](pool => DBAudit(jobRunId, logStackTrace, DBImpl(pool, fetchSize))))

  def apply(
      db: JDBC,
      jobRunId: String = UUID.randomUUID.toString,
      poolName: String = "EtlFlow-Audit-Pool",
      poolSize: Int = 2,
      fetchSize: Option[Int] = None,
      logStackTrace: Boolean = false
  ): TaskLayer[Audit] =
    etlflow.db.CP.layer(db, poolName, poolSize) >>> layer(jobRunId, fetchSize, logStackTrace)
}
