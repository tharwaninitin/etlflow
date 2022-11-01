package etlflow.audit

import etlflow.log.ApplicationLogger
import etlflow.model.Credential.JDBC
import etlflow.utils.{DateTimeApi, MapToJson}
import scalikejdbc.NamedDB
import zio.{TaskLayer, UIO, ZIO, ZLayer}

object DB extends ApplicationLogger {
  case class DBLogger(jobRunId: String, poolName: String) extends etlflow.audit.Audit {
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

    override def logJobStart(jobName: String, args: String, startTime: Long): UIO[Unit] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          Sql
            .insertJobRun(jobRunId, jobName, args, startTime)
            .update
            .apply()
        })
        .fold(e => logger.error(e.getMessage), _ => ())

    override def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): UIO[Unit] =
      ZIO
        .attempt(NamedDB(poolName).localTx { implicit s =>
          val status      = error.fold("pass")(ex => s"failed with error: ${ex.getMessage}")
          val elapsedTime = DateTimeApi.getTimeDifferenceAsString(endTime, DateTimeApi.getCurrentTimestamp)
          Sql
            .updateJobRun(jobRunId, status, elapsedTime)
            .update
            .apply()
        })
        .fold(e => logger.error(e.getMessage), _ => ())
  }

  private[etlflow] def live(jobRunId: String): ZLayer[String, Throwable, Audit] = ZLayer {
    for {
      poolName <- ZIO.service[String]
    } yield DBLogger(jobRunId, poolName)
  }

  def apply(db: JDBC, jobRunId: String, poolName: String = "Job-Pool", poolSize: Int = 2): TaskLayer[Audit] =
    etlflow.db.CP.layer(db, poolName, poolSize) >>> live(jobRunId)
}
