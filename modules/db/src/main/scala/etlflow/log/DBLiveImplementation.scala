package etlflow.log

import etlflow.db.Sql
import etlflow.schema.Credential.JDBC
import etlflow.utils.ApplicationLogger
import etlflow.utils.EtlflowError.DBException
import scalikejdbc.NamedDB
import zio.{Has, IO, Task, ZLayer}
import zio.blocking.Blocking

object DBLiveImplementation extends ApplicationLogger {
  private[etlflow] val dbLogLayer: ZLayer[Has[String], Throwable, DBLogEnv] = ZLayer.fromService { pool_name =>
    new etlflow.log.DBApi.Service {
      override def updateStepRun(job_run_id: String, step_name: String, props: String, status: String, elapsed_time: String): IO[DBException, Unit] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            Sql.updateStepRun(job_run_id, step_name, props, status, elapsed_time)
              .update
              .apply()
          }).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }).unit
      }
      override def insertStepRun(job_run_id: String, step_name: String, props: String, step_type: String, step_run_id: String, start_time: Long): IO[DBException, Unit] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            Sql.insertStepRun(job_run_id, step_name, props, step_type, step_run_id, start_time)
              .update
              .apply()
          }).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }).unit
      }
      override def insertJobRun(job_run_id: String, job_name: String, props: String, job_type: String, is_master: String, start_time: Long): IO[DBException, Unit] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
            Sql.insertJobRun(job_run_id, job_name, props, job_type, is_master, start_time)
              .update
              .apply()
          }).mapError({
          e =>
            logger.error(e.getMessage)
            DBException(e.getMessage)
        }).unit
      }
      override def updateJobRun(job_run_id: String, status: String, elapsed_time: String): IO[DBException, Unit] = {
        Task(
          NamedDB(pool_name) localTx { implicit s =>
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
  def apply(db: JDBC, pool_name: String = "EtlFlow-Pool", pool_size: Int = 2): ZLayer[Blocking, Throwable, DBLogEnv] =
    etlflow.db.Implementation.cpLayer(db, pool_name, pool_size) >>> dbLogLayer
}
