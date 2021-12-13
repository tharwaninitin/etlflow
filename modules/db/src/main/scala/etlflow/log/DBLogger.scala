package etlflow.log

import etlflow.db.Sql
import etlflow.schema.Credential.JDBC
import etlflow.utils.EtlflowError.DBException
import etlflow.utils.{ApplicationLogger, DateTimeApi, MapToJson}
import scalikejdbc.NamedDB
import zio.blocking.Blocking
import zio.{Has, Task, UIO, ZLayer}

object DBLogger extends ApplicationLogger {
  private[etlflow] val dbLogLayer: ZLayer[Has[String], Throwable, LogEnv] = ZLayer.fromService { pool_name =>
    new etlflow.log.Service {

      var job_run_id: String = null

      override def setJobRunId(jri: String): UIO[Unit] = UIO {
        job_run_id = jri
      }
      override def logStepStart(step_run_id: String, step_name: String, props: Map[String, String], step_type: String, start_time: Long): Task[Unit] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            Sql.insertStepRun(step_run_id, step_name, MapToJson(props), step_type, job_run_id, start_time)
              .update
              .apply()
          }).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }).unit
      }
      override def logStepEnd(step_run_id: String, step_name: String, props: Map[String, String], step_type: String, end_time: Long, error: Option[Throwable]): Task[Unit] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            val status = if (error.isEmpty) "pass" else "failed with error: " + error.get.getMessage
            val elapsed_time = DateTimeApi.getTimeDifferenceAsString(end_time, DateTimeApi.getCurrentTimestamp)
            Sql.updateStepRun(step_run_id, MapToJson(props), status, elapsed_time)
              .update
              .apply()
          }).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }).unit
      }
      override def logJobStart(job_run_id: String, job_name: String, args: String, start_time: Long): Task[Unit] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            Sql.insertJobRun(job_run_id, job_name, args, start_time)
              .update
              .apply()
          }).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }).unit
      }
      override def logJobEnd(job_run_id: String, job_name: String, args: String, end_time: Long, error: Option[Throwable]): Task[Unit] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            val status = if (error.isEmpty) "pass" else "failed with error: " + error.get.getMessage
            val elapsed_time = DateTimeApi.getTimeDifferenceAsString(end_time, DateTimeApi.getCurrentTimestamp)
            Sql.updateJobRun(job_run_id, status, elapsed_time)
              .update
              .apply()
          }).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }).unit
      }
    }
  }
  def apply(db: JDBC, pool_name: String = "Job-Pool", pool_size: Int = 2): ZLayer[Blocking, Throwable, LogEnv] =
    etlflow.db.Implementation.cpLayer(db, pool_name, pool_size) >>> dbLogLayer
}
