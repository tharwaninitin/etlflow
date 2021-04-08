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
import doobie.util.meta.Meta
import etlflow.log.ApplicationLogger
import etlflow.utils.EtlFlowHelper.Creds.{AWS, JDBC}
import etlflow.utils.JsonJackson
import org.postgresql.util.PGobject

object Update extends ApplicationLogger {

  // Uncomment this to see generated SQL queries in logs
  // implicit val dbLogger = CustomLogHandler()

  case class JsonString(str: String) extends AnyVal
  implicit val jsonMeta: Meta[JsonString] = Meta.Advanced.other[PGobject]("jsonb").timap[JsonString](o => JsonString(o.getValue))(a => {
    val o = new PGobject
    o.setType("jsonb")
    o.setValue(a.str)
    o
  })

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

  def updateJobState(args: EtlJobStateArgs,transactor: HikariTransactor[Task]): Task[Boolean] = {
    sql"UPDATE job SET is_active = ${args.state} WHERE job_name = ${args.name}"
      .update
      .run
      .transact(transactor).map(_ => args.state)
  }.mapError { e =>
    logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def addCredentials(args: CredentialsArgs, transactor: HikariTransactor[Task]): Task[Credentials] = {
    val credentialsDB = CredentialDB(
      args.name,
      args.`type` match {
        case JDBC => "jdbc"
        case AWS => "aws"
      },
      JsonJackson.convertToJsonByRemovingKeys(args.value.map(x => (x.key,x.value)).toMap, List.empty)
    )

    sql"INSERT INTO credentials (name,type,value) VALUES (${credentialsDB.name}, ${credentialsDB.`type`}, ${JsonString(credentialsDB.value)})"
      .update
      .run
      .transact(transactor)
      .map(_ => Credentials(credentialsDB.name,credentialsDB.`type`,credentialsDB.value))
  }.mapError { e =>
    logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def updateCredentials(args: CredentialsArgs,transactor: HikariTransactor[Task]): Task[Credentials] = {

    val value = JsonJackson.convertToJsonByRemovingKeys(args.value.map(x => (x.key,x.value)).toMap, List.empty)

    val credentialsDB = CredentialDB(
      args.name,
      args.`type` match {
        case JDBC => "jdbc"
        case AWS => "aws"
      },
      value
    )

    val updateQuery = sql"""
    UPDATE credentials
    SET valid_to = NOW() - INTERVAL '00:00:01'
    WHERE credentials.name = ${credentialsDB.name}
       AND credentials.valid_to IS NULL
    """.stripMargin.update.run

    val insertQuery = sql"""
    INSERT INTO credentials (name,type,value)
    VALUES (${credentialsDB.name},${credentialsDB.`type`},${JsonString(credentialsDB.value)});
    """.stripMargin.update.run

    val singleTran = for {
      _ <- updateQuery
      _ <- insertQuery
    } yield ()

    singleTran.transact(transactor).map(_ => Credentials(credentialsDB.name,credentialsDB.`type`,credentialsDB.value))

  }.mapError { e =>
    logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def addCronJob(args: CronJobArgs,transactor: HikariTransactor[Task]): Task[CronJob] = {
    val cronJobDB = JobDB(args.job_name,"", args.schedule.toString,0,0,is_active = true)
    sql"""INSERT INTO job (job_name,schedule,failed,success,is_active)
         VALUES (${cronJobDB.job_name}, ${cronJobDB.schedule}, ${cronJobDB.failed}, ${cronJobDB.success}, ${cronJobDB.is_active})"""
      .update
      .run
      .transact(transactor).map(_ => CronJob(cronJobDB.job_name,"",Cron(cronJobDB.schedule).toOption,0,0))
  }.mapError { e =>
    logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def updateCronJob(args: CronJobArgs,transactor: HikariTransactor[Task]): Task[CronJob] = {
    sql"UPDATE job SET schedule = ${args.schedule.toString} WHERE job_name = ${args.job_name}"
      .update.run.transact(transactor).map(_ => CronJob(args.job_name,"",Some(args.schedule),0,0))
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
