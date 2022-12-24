package etlflow.audit

import etlflow.log.ApplicationLogger
import etlflow.model.Credential.JDBC
import etlflow.utils.{DateTimeApi, MapToJson}
import scalikejdbc.NamedDB
import zio.{TaskLayer, UIO, ZIO, ZLayer}

object DB extends ApplicationLogger {
  private[etlflow] case class DBAudit(jobRunId: String, poolName: String) extends etlflow.audit.Audit {
    override def logTaskStart(
        taskRunId: String,
        taskName: String,
        props: Map[String, String],
        taskType: String,
        startTime: Long
    ): UIO[Unit] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          Sql
            .insertTaskRun(taskRunId, taskName, MapToJson(props), taskType, jobRunId, startTime)
            .update
            .apply()
        })
        .fold(e => logger.error(e.getMessage), _ => ())

    override def logTaskEnd(
        taskRunId: String,
        taskName: String,
        props: Map[String, String],
        taskType: String,
        endTime: Long,
        error: Option[Throwable]
    ): UIO[Unit] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          val status      = error.fold("pass")(ex => s"failed with error: ${ex.getMessage}")
          val elapsedTime = DateTimeApi.getTimeDifferenceAsString(endTime, DateTimeApi.getCurrentTimestamp)
          Sql
            .updateTaskRun(taskRunId, MapToJson(props), status, elapsedTime)
            .update
            .apply()
        })
        .fold(e => logger.error(e.getMessage), _ => ())

    override def logJobStart(jobName: String, args: Map[String, String], props: Map[String, String], startTime: Long): UIO[Unit] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          val arguments  = MapToJson(args)
          val properties = MapToJson(props)
          Sql
            .insertJobRun(jobRunId, jobName, arguments, properties, startTime)
            .update
            .apply()
        })
        .fold(e => logger.error(e.getMessage), _ => ())

    override def logJobEnd(
        jobName: String,
        args: Map[String, String],
        props: Map[String, String],
        endTime: Long,
        error: Option[Throwable]
    ): UIO[Unit] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          val status      = error.fold("pass")(ex => s"failed with error: ${ex.getMessage}")
          val elapsedTime = DateTimeApi.getTimeDifferenceAsString(endTime, DateTimeApi.getCurrentTimestamp)
          val properties  = MapToJson(props)
          Sql
            .updateJobRun(jobRunId, status, properties, elapsedTime)
            .update
            .apply()
        })
        .fold(e => logger.error(e.getMessage), _ => ())
  }

  private[etlflow] def layer(jobRunId: String): ZLayer[String, Throwable, Audit] =
    ZLayer(ZIO.service[String].map(pool => DBAudit(jobRunId, pool)))

  def apply(db: JDBC, jobRunId: String, poolName: String = "EtlFlow-Audit-Pool", poolSize: Int = 2): TaskLayer[Audit] =
    etlflow.db.CP.layer(db, poolName, poolSize) >>> layer(jobRunId)
}
