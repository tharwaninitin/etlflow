package etlflow.log

import etlflow.model.Credential.JDBC
import etlflow.utils.{ApplicationLogger, DateTimeApi, MapToJson}
import scalikejdbc.NamedDB
import zio.blocking.Blocking
import zio.{Has, Task, UIO, ZLayer}

object DB extends ApplicationLogger {
  case class DBLogger(jobRunId: String, poolName: String) extends etlflow.log.Service {
    override def logStepStart(
        stepRunId: String,
        stepName: String,
        props: Map[String, String],
        stepType: String,
        startTime: Long
    ): UIO[Unit] =
      Task(NamedDB(poolName).localTx { implicit s =>
        Sql
          .insertStepRun(stepRunId, stepName, MapToJson(props), stepType, jobRunId, startTime)
          .update
          .apply()
      }).fold(e => logger.error(e.getMessage), _ => ())

    override def logStepEnd(
        stepRunId: String,
        stepName: String,
        props: Map[String, String],
        stepType: String,
        endTime: Long,
        error: Option[Throwable]
    ): UIO[Unit] =
      Task(NamedDB(poolName).localTx { implicit s =>
        val status      = error.fold("pass")(ex => s"failed with error: ${ex.getMessage}")
        val elapsedTime = DateTimeApi.getTimeDifferenceAsString(endTime, DateTimeApi.getCurrentTimestamp)
        Sql
          .updateStepRun(stepRunId, MapToJson(props), status, elapsedTime)
          .update
          .apply()
      }).fold(e => logger.error(e.getMessage), _ => ())

    override def logJobStart(jobName: String, args: String, startTime: Long): UIO[Unit] =
      Task(NamedDB(poolName).localTx { implicit s =>
        Sql
          .insertJobRun(jobRunId, jobName, args, startTime)
          .update
          .apply()
      }).fold(e => logger.error(e.getMessage), _ => ())

    override def logJobEnd(jobName: String, args: String, endTime: Long, error: Option[Throwable]): UIO[Unit] =
      Task(NamedDB(poolName).localTx { implicit s =>
        val status      = error.fold("pass")(ex => s"failed with error: ${ex.getMessage}")
        val elapsedTime = DateTimeApi.getTimeDifferenceAsString(endTime, DateTimeApi.getCurrentTimestamp)
        Sql
          .updateJobRun(jobRunId, status, elapsedTime)
          .update
          .apply()
      }).fold(e => logger.error(e.getMessage), _ => ())
  }

  private[etlflow] def live(jobRunId: String): ZLayer[Has[String], Throwable, LogEnv] =
    ZLayer.fromService(poolName => DBLogger(jobRunId, poolName))

  def apply(
      db: JDBC,
      jobRunId: String,
      poolName: String = "Job-Pool",
      poolSize: Int = 2
  ): ZLayer[Blocking, Throwable, LogEnv] =
    etlflow.db.CP.layer(db, poolName, poolSize) >>> live(jobRunId)
}
