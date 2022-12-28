package etlflow.audit

import etlflow.log.ApplicationLogger
import etlflow.model.Credential.JDBC
import etlflow.model.{JobRun, TaskRun}
import etlflow.utils.MapToJson
import scalikejdbc.NamedDB
import zio.{TaskLayer, UIO, ZIO, ZLayer}

object DB extends ApplicationLogger {
  private[etlflow] case class DBAudit(jobRunId: String, poolName: String) extends etlflow.audit.Audit {
    override def logTaskStart(
        taskRunId: String,
        taskName: String,
        props: Map[String, String],
        taskType: String
    ): UIO[Unit] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          Sql
            .insertTaskRun(taskRunId, taskName, MapToJson(props), taskType, jobRunId)
            .update
            .apply()
        })
        .fold(e => logger.error(e.getMessage), _ => ())

    override def logTaskEnd(
        taskRunId: String,
        taskName: String,
        props: Map[String, String],
        taskType: String,
        error: Option[Throwable]
    ): UIO[Unit] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          val status = error.fold("pass")(ex => s"failed with error: ${ex.getMessage}")
          Sql
            .updateTaskRun(taskRunId, MapToJson(props), status)
            .update
            .apply()
        })
        .fold(e => logger.error(e.getMessage), _ => ())

    override def logJobStart(jobName: String, props: Map[String, String]): UIO[Unit] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          val properties = MapToJson(props)
          Sql
            .insertJobRun(jobRunId, jobName, properties)
            .update
            .apply()
        })
        .fold(e => logger.error(e.getMessage), _ => ())

    override def logJobEnd(
        jobName: String,
        props: Map[String, String],
        error: Option[Throwable]
    ): UIO[Unit] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          val status     = error.fold("pass")(ex => s"failed with error: ${ex.getMessage}")
          val properties = MapToJson(props)
          Sql
            .updateJobRun(jobRunId, status, properties)
            .update
            .apply()
        })
        .fold(e => logger.error(e.getMessage), _ => ())

    override def getJobRuns(query: String): UIO[Iterable[JobRun]] = ZIO
      .attempt(NamedDB(poolName).localTx { implicit s =>
        scalikejdbc
          .SQL(query)
          .map(rs =>
            JobRun(
              rs.string("job_run_id"),
              rs.string("job_name"),
              rs.string("props"),
              rs.string("status"),
              rs.zonedDateTime("created_at"),
              rs.zonedDateTime("updated_at")
            )
          )
          .list()
      })
      .fold(
        { e =>
          logger.error(e.getMessage)
          List.empty
        },
        op => op
      )

    override def getTaskRuns(query: String): UIO[Iterable[TaskRun]] = ZIO
      .attempt(NamedDB(poolName).localTx { implicit s =>
        scalikejdbc
          .SQL(query)
          .map(rs =>
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
          )
          .list()
      })
      .fold(
        { e =>
          logger.error(e.getMessage)
          List.empty
        },
        op => op
      )
  }

  private[etlflow] def layer(jobRunId: String): ZLayer[String, Throwable, Audit] =
    ZLayer(ZIO.service[String].map(pool => DBAudit(jobRunId, pool)))

  def apply(db: JDBC, jobRunId: String, poolName: String = "EtlFlow-Audit-Pool", poolSize: Int = 2): TaskLayer[Audit] =
    etlflow.db.CP.layer(db, poolName, poolSize) >>> layer(jobRunId)
}
