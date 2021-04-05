package etlflow.utils.db

import caliban.CalibanError.ExecutionError
import cats.data.NonEmptyList
import cats.effect.Async
import cron4s.Cron
import doobie.hikari.HikariTransactor
import etlflow.utils.EtlFlowHelper._
import zio.{IO, Task}
import doobie.implicits._
import zio.interop.catz._
import doobie.ConnectionIO
import etlflow.log.ApplicationLogger

object Update extends ApplicationLogger {

  def updateSuccessJob(job: String, transactor: HikariTransactor[Task]): IO[ExecutionError, Long] = {
    sql"UPDATE job SET success = (success + 1) WHERE job_name = ${job}"
      .update
      .run
      .transact(transactor)
      .map(_ => 1L)
    }.mapError { e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
    }

  def updateFailedJob(job: String, transactor: HikariTransactor[Task]): IO[ExecutionError, Long] = {
    sql"UPDATE job SET failed = (failed + 1) WHERE job_name = $job"
      .update
      .run
      .transact(transactor)
      .map(_ => 1L)
    }.mapError { e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
    }

  private def deleteJobs(jobs: List[JobDB]): ConnectionIO[Int] = {
    val list = NonEmptyList(jobs.head,jobs.tail).map(x => x.job_name)
    val query = fr"DELETE FROM job WHERE " ++ doobie.util.fragments.notIn(fr"job_name", list)
    query.update.run
  }

  private def insertJobs(jobs: List[JobDB]): ConnectionIO[Int] = {
    val sql = """
       INSERT INTO job AS t (job_name,job_description,schedule,failed,success,is_active)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT (job_name)
       DO UPDATE SET schedule = EXCLUDED.schedule
    """
    doobie.util.update.Update[JobDB](sql).updateMany(jobs)
  }

  private val selectJobs: ConnectionIO[List[JobDB]] = {
    sql"""
       SELECT job_name, job_description, schedule, failed, success, is_active
       FROM job
       """
      .query[JobDB]
      .to[List]
  }

  private def logGeneral[F[_]: Async](print: Any): F[Unit] = Async[F].pure(logger.info(print.toString))

  private def logCIO(print: Any): ConnectionIO[Unit] = logGeneral[ConnectionIO](print)

  def refreshJobs(transactor: HikariTransactor[Task], jobs: List[JobDB]): IO[ExecutionError, List[CronJob]] = {
    val singleTran = for {
      _        <- logCIO(s"Refreshing jobs in database")
      deleted  <- deleteJobs(jobs)
      _        <- logCIO(s"Deleted jobs => $deleted")
      inserted <- insertJobs(jobs)
      _        <- logCIO(s"Inserted/Updated jobs => $inserted")
      db_jobs  <- selectJobs
      jobs     = db_jobs.map(x => CronJob(x.job_name,x.job_description, Cron(x.schedule).toOption, x.failed, x.success))
    } yield jobs

    singleTran
      .transact(transactor)
      .mapError { e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
  }
}
