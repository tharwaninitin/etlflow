package etlflow.log

import etlflow.model.Credential.JDBC
import etlflow.utils.{ApplicationLogger, DateTimeApi, MapToJson}
import scalikejdbc.NamedDB
import zio.blocking.Blocking
import zio.{Has, Task, UIO, ZLayer}

object DB extends ApplicationLogger {
  case class DBLogger(job_run_id: String, pool_name: String) extends etlflow.log.Service {
    override def logStepStart(
        step_run_id: String,
        step_name: String,
        props: Map[String, String],
        step_type: String,
        start_time: Long
    ): UIO[Unit] =
      Task(NamedDB(pool_name).localTx { implicit s =>
        Sql
          .insertStepRun(step_run_id, step_name, MapToJson(props), step_type, job_run_id, start_time)
          .update
          .apply()
      }).fold(e => logger.error(e.getMessage), _ => ())

    override def logStepEnd(
        step_run_id: String,
        step_name: String,
        props: Map[String, String],
        step_type: String,
        end_time: Long,
        error: Option[Throwable]
    ): UIO[Unit] =
      Task(NamedDB(pool_name).localTx { implicit s =>
        val status       = if (error.isEmpty) "pass" else "failed with error: " + error.get.getMessage
        val elapsed_time = DateTimeApi.getTimeDifferenceAsString(end_time, DateTimeApi.getCurrentTimestamp)
        Sql
          .updateStepRun(step_run_id, MapToJson(props), status, elapsed_time)
          .update
          .apply()
      }).fold(e => logger.error(e.getMessage), _ => ())
    override def logJobStart(job_name: String, args: String, start_time: Long): UIO[Unit] =
      Task(NamedDB(pool_name).localTx { implicit s =>
        Sql
          .insertJobRun(job_run_id, job_name, args, start_time)
          .update
          .apply()
      }).fold(e => logger.error(e.getMessage), _ => ())
    override def logJobEnd(job_name: String, args: String, end_time: Long, error: Option[Throwable]): UIO[Unit] =
      Task(NamedDB(pool_name).localTx { implicit s =>
        val status       = if (error.isEmpty) "pass" else "failed with error: " + error.get.getMessage
        val elapsed_time = DateTimeApi.getTimeDifferenceAsString(end_time, DateTimeApi.getCurrentTimestamp)
        Sql
          .updateJobRun(job_run_id, status, elapsed_time)
          .update
          .apply()
      }).fold(e => logger.error(e.getMessage), _ => ())
  }

  private[etlflow] def live(job_run_id: String): ZLayer[Has[String], Throwable, LogEnv] =
    ZLayer.fromService(pool_name => DBLogger(job_run_id, pool_name))

  def apply(
      db: JDBC,
      job_run_id: String,
      pool_name: String = "Job-Pool",
      pool_size: Int = 2
  ): ZLayer[Blocking, Throwable, LogEnv] =
    etlflow.db.CP.layer(db, pool_name, pool_size) >>> live(job_run_id)
}
