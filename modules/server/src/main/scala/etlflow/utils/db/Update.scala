package etlflow.utils.db

import caliban.CalibanError.ExecutionError
import cron4s.Cron
import doobie.hikari.HikariTransactor
import etlflow.utils.EtlFlowHelper._
import zio.{IO, Task}
import doobie.implicits._
import org.slf4j.{Logger, LoggerFactory}
import zio.interop.catz._
import doobie.ConnectionIO
object Update extends doobie.Aliases {

  lazy val update_logger: Logger = LoggerFactory.getLogger(getClass.getName)


  def updateSuccessJob(job: String, transactor: HikariTransactor[Task]): IO[ExecutionError, Long] = {

    sql"UPDATE job SET success = (success + 1) WHERE job_name = ${job}"
      .update
      .run
      .transact(transactor)
      .map(_ => 1L)
  }.mapError { e =>
    update_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def updateFailedJob(job: String, transactor: HikariTransactor[Task]): IO[ExecutionError, Long] = {

    sql"UPDATE job SET failed = (failed + 1) WHERE job_name = ${job}"
      .update
      .run
      .transact(transactor)
      .map(_ => 1L)

  }.mapError { e =>
    update_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def deleteJobs(transactor: HikariTransactor[Task], jobList: List[String]): Task[Int] = {
    val query = "DELETE FROM job WHERE job_name NOT IN " + "('" + jobList.mkString("','") + "')"
    Fragment.const(query).update.run.transact(transactor)
  }

  def insertJobDb(cronJobsDb: List[JobDB]): ConnectionIO[Int] = {
    val sql =
      """
       INSERT INTO job AS t (job_name,job_description,schedule,failed,success,is_active)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT (job_name)
       DO UPDATE SET schedule = EXCLUDED.schedule
    """
    Update[JobDB](sql).updateMany(cronJobsDb)
  }

  def updateJobs(transactor: HikariTransactor[Task], cronJobsDb: List[JobDB]): Task[List[CronJob]] = {

    insertJobDb(cronJobsDb).transact(transactor)

    val selectQuery = sql"""SELECT job_name, job_description, schedule, failed, success, is_active FROM job"""
      .query[JobDB]
      .to[List]

    insertJobDb(cronJobsDb).transact(transactor) *> selectQuery.transact(transactor)
          .map(y => y.map(x => CronJob(x.job_name,x.job_description, Cron(x.schedule).toOption, x.failed, x.success)))
  }
}
