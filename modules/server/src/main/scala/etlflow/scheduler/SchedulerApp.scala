package etlflow.scheduler

import cats.effect.Blocker
import cron4s.CronExpr
import doobie.hikari.HikariTransactor
import etlflow.executor.Executor
import etlflow.utils.EtlFlowHelper._
import etlflow.utils.db.Update
import etlflow.utils.{EtlFlowUtils, UtilityFunctions => UF}
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.{EtlFlowApp, EtlJobProps, EtlJobPropsMapping}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

import scala.reflect.runtime.universe.TypeTag

abstract class SchedulerApp[EJN <: EtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]] : TypeTag]
  extends EtlFlowApp[EJN]
    with Executor
    with EtlFlowUtils {

  final def scheduleJobs(dbCronJobs: List[CronJob], transactor: HikariTransactor[Task], jobSemaphores: Map[String, Semaphore], jobQueue:Queue[(String,String,String,String)]): RIO[Blocking with Clock,Unit] = {
    if (dbCronJobs.isEmpty) {
      logger.warn("No scheduled jobs found")
      ZIO.unit
    }
    else {
      val listOfCron: List[(CronExpr, URIO[Blocking with Clock, Option[EtlJob]])] = dbCronJobs.map(cj => (cj.schedule.get, {
        logger.info(s"Scheduling job ${cj.job_name} with schedule ${cj.schedule.get.toString} at ${UF.getCurrentTimestampAsString()}")
        runActiveEtlJob[EJN](EtlJobArgs(cj.job_name),transactor,jobSemaphores(cj.job_name),config,etl_job_props_mapping_package,"Scheduler-API",jobQueue).map(Some(_)).
          catchAll(_ => UIO.succeed(None))
      }))

      val scheduledJobs = repeatEffectsForCron(listOfCron)

      val scheduledLogger = UIO(logger.info("*"*30 + s" Scheduler heartbeat at ${UF.getCurrentTimestampAsString()} " + "*"*30))
        .repeat(Schedule.forever && Schedule.spaced(60.minute))

      scheduledJobs.zipPar(scheduledLogger).as(())
        .tapError{e =>
          UIO(logger.error("*"*30 + s" Scheduler crashed due to error ${e.getMessage} stacktrace ${e.printStackTrace()}" + "*"*30))
        }
    }
  }

  final def etlFlowScheduler(transactor: HikariTransactor[Task], cronJobs: Ref[List[CronJob]], jobs: List[EtlJob], jobSemaphores: Map[String, Semaphore],jobQueue:Queue[(String,String,String,String)]): RIO[Blocking with Clock, Unit] = for {
    dbJobs     <- refreshJobsDB(transactor,jobs)
    _          <- cronJobs.update{_ => dbJobs.filter(_.schedule.isDefined)}
    cronJobs   <- cronJobs.get
    _          <- UIO(logger.info(s"Refreshed jobs in database \n${dbJobs.mkString("\n")}"))
    _          <- UIO(logger.info("Starting scheduler"))
    _          <- scheduleJobs(cronJobs,transactor,jobSemaphores,jobQueue)
  } yield ()

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val schedulerRunner: ZIO[ZEnv, Throwable, Unit] = (for {
      blocker         <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
      transactor      <- createDbTransactorManaged(config.dbLog, platform.executor.asEC, "EtlFlowScheduler-Pool", 10)(blocker)
      jobs            <- getEtlJobs[EJN](etl_job_props_mapping_package).toManaged_
      cronJobs        <- Ref.make(List.empty[CronJob]).toManaged_
      jobSemaphores   <- createSemaphores(jobs).toManaged_
      queue           <- Queue.sliding[(String,String,String,String)](20).toManaged_
      _               <- etlFlowScheduler(transactor,cronJobs,jobs,jobSemaphores,queue).toManaged_
    } yield ()).use_(ZIO.unit)

    val finalRunner = if (args.isEmpty) schedulerRunner else cliRunner(args)

    finalRunner.catchAll{err =>
      UIO {
        logger.error(err.getMessage)
        err.getStackTrace.foreach(x => logger.error(x.toString))
      }
    }.exitCode
  }
}
