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
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def log[R](effect: RIO[R, Unit]): URIO[R, Unit] =
    effect.fold(
      e => {
        logger.error(e.getMessage)
        e.getStackTrace.foreach(st => logger.error(st.toString))
      },
      _ => ()
    )

  private[etlflow] case class DBAudit(jobRunId: String, client: DBImpl) extends etlflow.audit.Audit {
    override def logTaskStart(taskRunId: String, taskName: String, props: String, taskType: String): UIO[Unit] = log(
      client
        .executeQuery(Sql.insertTaskRun(taskRunId, taskName, props, taskType, jobRunId))
    )

    override def logTaskEnd(taskRunId: String, error: Option[Throwable]): UIO[Unit] = log(
      client
        .executeQuery(Sql.updateTaskRun(taskRunId, error.fold("success")(ex => s"failed with error: ${ex.getMessage}")))
    )

    override def logJobStart(jobName: String, props: String): UIO[Unit] = log(
      client
        .executeQuery(Sql.insertJobRun(jobRunId, jobName, props))
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

    override def fetchResults[RS, T](query: String)(fn: RS => T): Task[Iterable[T]] =
      client.fetchResults(query)(fn.asInstanceOf[WrappedResultSet => T])

    override def executeQuery(query: String): Task[Unit] = client.executeQuery(query)
  }

  private[etlflow] def layer(jobRunId: String, fetchSize: Option[Int]): ZLayer[String, Throwable, Audit] =
    ZLayer(ZIO.service[String].map(pool => DBAudit(jobRunId, DBImpl(pool, fetchSize))))

  def apply(
      db: JDBC,
      jobRunId: String = UUID.randomUUID.toString,
      poolName: String = "EtlFlow-Audit-Pool",
      poolSize: Int = 2,
      fetchSize: Option[Int] = None
  ): TaskLayer[Audit] =
    etlflow.db.CP.layer(db, poolName, poolSize) >>> layer(jobRunId, fetchSize)
}
