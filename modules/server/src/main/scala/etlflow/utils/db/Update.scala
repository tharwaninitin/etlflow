package etlflow.utils.db

import caliban.CalibanError.ExecutionError
import cron4s.Cron
import doobie.hikari.HikariTransactor
import doobie.quill.DoobieContext
import etlflow.utils.EtlFlowHelper._
import io.getquill.Literal
import zio.{IO, Task}
import doobie.implicits._
import doobie.util.fragment.Fragment
import org.slf4j.{Logger, LoggerFactory}
import zio.interop.catz._

object Update {

  lazy val update_logger: Logger = LoggerFactory.getLogger(getClass.getName)
  val dc = new DoobieContext.Postgres(Literal)
  import dc._

  def updateSuccessJob(job: String, transactor: HikariTransactor[Task]): IO[ExecutionError, Long] = {
    val cronJobStringUpdate = quote {
      querySchema[CronJobDB]("cronjob")
        .filter(x => x.job_name == lift(job))
        .update{cj =>
          cj.success -> (cj.success + 1L)
        }
    }
    dc.run(cronJobStringUpdate).transact(transactor)
  }.mapError { e =>
    update_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def updateFailedJob(job: String, transactor: HikariTransactor[Task]): IO[ExecutionError, Long] = {
    val cronJobStringUpdate = quote {
      querySchema[CronJobDB]("cronjob")
        .filter(x => x.job_name == lift(job))
        .update{cj =>
          cj.failed -> (cj.failed + 1L)
        }
    }
    dc.run(cronJobStringUpdate).transact(transactor)
  }.mapError { e =>
    update_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }

  def deleteCronJobsDB(transactor: HikariTransactor[Task], jobList: List[String]): Task[Int] = {
    val query = "DELETE FROM cronjob WHERE job_name NOT IN " +  "('" + jobList.mkString("','") +  "')"
    Fragment.const(query).update.run.transact(transactor)
  }

  def updateCronJobsDB(transactor: HikariTransactor[Task], cronJobsDb: Set[CronJobDB]): Task[List[CronJob]] = {
    val insertQuery = quote {
      liftQuery(cronJobsDb).foreach{e =>
        querySchema[CronJobDB]("cronjob")
          .insert(e)
          .onConflictUpdate(_.job_name)(
            _.schedule -> _.schedule
          )
      }
    }
    dc.run(insertQuery).transact(transactor)

    val selectQuery = quote {
      querySchema[CronJobDB]("cronjob")
    }

    dc.run(insertQuery).transact(transactor) *> dc.run(selectQuery).transact(transactor)
      .map(y => y.map(x => CronJob(x.job_name, Cron(x.schedule).toOption, x.failed, x.success)))
  }
}
