package etlflow.scheduler

import caliban.CalibanError.ExecutionError
import cron4s.Cron
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.quill.DoobieContext
import doobie.util.fragment.Fragment
import etlflow.scheduler.api.EtlFlowHelper._
import etlflow.utils.Executor.{DATAPROC, LOCAL}
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import eu.timepit.fs2cron.schedule
import fs2.Stream
import io.getquill.Literal
import org.slf4j.{Logger, LoggerFactory}
import zio._
import zio.interop.catz._
import zio.interop.catz.implicits._
import scala.reflect.runtime.universe.TypeTag

abstract class Scheduler[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag] {

  lazy val scheduler_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val etl_job_name_package: String
  val scheduler_dc = new DoobieContext.Postgres(Literal)
  import scheduler_dc._

  final def getCronJobFromDB(name: String, transactor: HikariTransactor[Task]): IO[ExecutionError, CronJobDB] = {
    val cj = quote {
      querySchema[CronJobDB]("cronjob")
        .filter(x => x.job_name == lift(name))
    }
    scheduler_dc.run(cj).transact(transactor).map(x => x.head)
  }.mapError { e =>
    scheduler_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }
  final def updateSuccessJob(job: String, transactor: HikariTransactor[Task]): IO[ExecutionError, Long] = {
    val cronJobStringUpdate = quote {
      querySchema[CronJobDB]("cronjob")
        .filter(x => x.job_name == lift(job))
        .update{cj =>
          cj.success -> (cj.success + 1L)
        }
    }
    scheduler_dc.run(cronJobStringUpdate).transact(transactor)
  }.mapError { e =>
    scheduler_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }
  final def updateFailedJob(job: String, transactor: HikariTransactor[Task]): IO[ExecutionError, Long] = {
    val cronJobStringUpdate = quote {
      querySchema[CronJobDB]("cronjob")
        .filter(x => x.job_name == lift(job))
        .update{cj =>
          cj.failed -> (cj.failed + 1L)
        }
    }
    scheduler_dc.run(cronJobStringUpdate).transact(transactor)
  }.mapError { e =>
    scheduler_logger.error(e.getMessage)
    ExecutionError(e.getMessage)
  }
  final def runActiveEtlJobRemote(args: EtlJobArgs, transactor: HikariTransactor[Task],dataproc: DATAPROC): Task[Unit] = {
    for {
      _  <- UIO(scheduler_logger.info(s"Checking if job  ${args.name} is active/incative at ${UF.getCurrentTimestampAsString()}"))
      cj <- getCronJobFromDB(args.name,transactor)
      _  <- if (cj.is_active) UIO(s"Running job ${cj.job_name} with schedule ${cj.schedule} at ${UF.getCurrentTimestampAsString()}") *> runEtlJobRemote(args,transactor,dataproc)
      else UIO(
        scheduler_logger.info(
          s"Skipping inactive cron job ${cj.job_name} with schedule " +
            s"${cj.schedule} at ${UF.getCurrentTimestampAsString()}"
        )
      )
    } yield ()
  }
  final def runActiveEtlJobLocal(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[Unit] = {
    for {
      _  <- UIO(scheduler_logger.info(s"Checking if job  ${args.name} is active/incative at ${UF.getCurrentTimestampAsString()}"))
      cj <- getCronJobFromDB(args.name,transactor)
      _  <- if (cj.is_active) UIO(s"Running job ${cj.job_name} with schedule ${cj.schedule} at ${UF.getCurrentTimestampAsString()}") *> runEtlJobLocal(args,transactor) else UIO(
        scheduler_logger.info(
          s"Skipping inactive cron job ${cj.job_name} with schedule " +
            s"${cj.schedule} at ${UF.getCurrentTimestampAsString()}"
        )
      )
    } yield ()
  }
  final def deleteCronJobsDB(transactor: HikariTransactor[Task]):Task[Int] = {
    val query = "DELETE FROM cronjob WHERE job_name NOT IN " +  "('" + UF.getEtlJobs[EJN].map(x => x).toList.mkString("','") +  "')"
    scheduler_logger.info("query : "+ query)
    Fragment.const(query).update.run.transact(transactor)
  }
  final def updateCronJobsDB(transactor: HikariTransactor[Task]): Task[List[CronJob]] = {

    val cronJobsDb = UF.getEtlJobs[EJN].map{x =>
      CronJobDB(
        x,
        UF.getEtlJobName[EJN](x,etl_job_name_package).getActualProperties(Map.empty).job_schedule,
        0,
        0,
        true
      )
    }

    val insertQuery = quote {
      liftQuery(cronJobsDb).foreach{e =>
        querySchema[CronJobDB]("cronjob")
          .insert(e)
          .onConflictUpdate(_.job_name)(
            _.schedule -> _.schedule
          )
      }
    }
    scheduler_dc.run(insertQuery).transact(transactor)

    val selectQuery = quote {
      querySchema[CronJobDB]("cronjob")
    }

    scheduler_dc.run(insertQuery).transact(transactor) *> scheduler_dc.run(selectQuery).transact(transactor)
      .map(y => y.map(x => CronJob(x.job_name, Cron(x.schedule).toOption, x.failed, x.success)))
  }
  final def scheduledTask(dbCronJobs: List[CronJob], transactor: HikariTransactor[Task]): Task[Unit] = {
    val jobsToBeScheduled = dbCronJobs.flatMap{ cj =>
      if (cj.schedule.isDefined)
        List(cj)
      else
        List.empty
    }

    val cronSchedule = schedule(jobsToBeScheduled.map(cj => (cj.schedule.get,Stream.eval {
      val job_deploy_mode = UF.getEtlJobName[EJN](cj.job_name, etl_job_name_package).getActualProperties(Map.empty).job_deploy_mode

      job_deploy_mode match {
        case DATAPROC(project, region, endpoint, cluster_name) =>
          scheduler_logger.info(
            s"Scheduled cron job ${cj.job_name} with schedule ${cj.schedule.get.toString} " +
              s"at ${UF.getCurrentTimestampAsString()} in remote mode "
          )
          runActiveEtlJobRemote(EtlJobArgs(cj.job_name, List.empty), transactor, DATAPROC(project,region,endpoint,cluster_name))
        case LOCAL =>
          scheduler_logger.info(
            s"Scheduled cron job ${cj.job_name} with schedule ${cj.schedule.get.toString} " +
              s"at ${UF.getCurrentTimestampAsString()} in local mode "
          )
          runActiveEtlJobLocal(EtlJobArgs(cj.job_name, List.empty), transactor)
        case _ =>
          scheduler_logger.warn(s"Job not scheduled due to incorrect job_deploy_mode for job ${cj.job_name}, allowed values are local,remote")
          ZIO.unit
      }
    })))
    cronSchedule.compile.drain
  }

  def runEtlJobRemote(args: EtlJobArgs, transactor: HikariTransactor[Task], config: DATAPROC): Task[EtlJob]
  def runEtlJobLocal(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[EtlJob]

  def etlFlowScheduler(transactor: HikariTransactor[Task], cronJobs: Ref[List[CronJob]]): Task[Unit] = for {
      _                 <- deleteCronJobsDB(transactor)
      dbCronJobs        <- updateCronJobsDB(transactor)
      _                 <- cronJobs.update{_ => dbCronJobs.filter(_.schedule.isDefined)}
      _                 <- Task(scheduler_logger.info(s"Added/Updated jobs in database \n${dbCronJobs.mkString("\n")}"))
      scheduledFork     <- scheduledTask(dbCronJobs,transactor)
    } yield ()
}
