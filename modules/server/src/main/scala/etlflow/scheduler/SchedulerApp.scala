package etlflow.scheduler

import cats.effect.Blocker
import doobie.hikari.HikariTransactor
import etlflow.executor.Executor
import etlflow.utils.EtlFlowHelper._
import etlflow.utils.db.Update
import etlflow.utils.{EtlFlowUtils, UtilityFunctions => UF}
import etlflow.{EtlFlowApp, EtlJobName, EtlJobProps}
import eu.timepit.fs2cron.schedule
import fs2.Stream
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.interop.catz._
import zio.interop.catz.implicits._
import scala.reflect.runtime.universe.TypeTag

abstract class SchedulerApp[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag]
  extends EtlFlowApp[EJN,EJP]
    with Executor
    with EtlFlowUtils {

  final def refreshCronJobsDB(transactor: HikariTransactor[Task]): Task[List[CronJob]] = {
    val cronJobsDb = UF.getEtlJobs[EJN].map{x =>
      CronJobDB(
        x,
        UF.getEtlJobName[EJN](x,etl_job_name_package).getActualProperties(Map.empty).job_schedule,
        0,
        0,
        true
      )
    }
    Update.updateCronJobsDB(transactor, cronJobsDb)
  }

  final def scheduledTask(dbCronJobs: List[CronJob], transactor: HikariTransactor[Task], jobSemaphores: Map[String, Semaphore],jobQueue:Queue[(String,String)]): Task[Unit] = {
    val jobsToBeScheduled = dbCronJobs.flatMap{ cj =>
      if (cj.schedule.isDefined)
        List(cj)
      else
        List.empty
    }
    if (jobsToBeScheduled.isEmpty) {
      logger.warn("No scheduled jobs found")
      ZIO.unit
    }
    else {
      val cronSchedule = schedule(jobsToBeScheduled.map(cj => (cj.schedule.get,Stream.eval {
        logger.info(s"Scheduling job ${cj.job_name} with schedule ${cj.schedule.get.toString} at ${UF.getCurrentTimestampAsString()}")
        runActiveEtlJob[EJN,EJP](EtlJobArgs(cj.job_name,List.empty),transactor,jobSemaphores(cj.job_name),config,etl_job_name_package,"Scheduler",jobQueue)
      })))

      UIO(logger.info("*"*30 + s" Scheduler heartbeat at ${UF.getCurrentTimestampAsString()} " + "*"*30))
        .repeat(Schedule.forever && Schedule.spaced(60.minute))
        .provideLayer(Clock.live).fork *> cronSchedule.compile.drain
        .mapError{e =>
          logger.error("*"*30 + s" Scheduler crashed due to error ${e.getMessage} stacktrace ${e.printStackTrace()}" + "*"*30)
          e
        }
    }
  }

  final def etlFlowScheduler(transactor: HikariTransactor[Task], cronJobs: Ref[List[CronJob]], jobSemaphores: Map[String, Semaphore],jobQueue:Queue[(String,String)]): Task[Unit] = for {
    _          <- Update.deleteCronJobsDB(transactor,UF.getEtlJobs[EJN].map(x => x).toList)
    dbCronJobs <- refreshCronJobsDB(transactor)
    _          <- cronJobs.update{_ => dbCronJobs.filter(_.schedule.isDefined)}
    _          <- UIO(logger.info(s"Refreshed jobs in database \n${dbCronJobs.mkString("\n")}"))
    _          <- UIO(logger.info("Starting scheduler"))
    _          <- scheduledTask(dbCronJobs,transactor,jobSemaphores,jobQueue)
  } yield ()

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    val schedulerRunner: ZIO[ZEnv, Throwable, Unit] = (for {
      blocker         <- ZIO.access[Blocking](_.get.blockingExecutor.asEC).map(Blocker.liftExecutionContext).toManaged_
      transactor      <- createDbTransactorManaged(config.dbLog, platform.executor.asEC, "EtlFlowScheduler-Pool", 10)(blocker)
      cronJobs        <- Ref.make(List.empty[CronJob]).toManaged_
      jobs            <- getEtlJobs[EJN,EJP](etl_job_name_package).toManaged_
      jobSemaphores   <- createSemaphores(jobs).toManaged_
      queue           <- Queue.sliding[(String,String)](20).toManaged_
      _               <- etlFlowScheduler(transactor,cronJobs,jobSemaphores,queue).toManaged_
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
