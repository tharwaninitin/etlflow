package etlflow.scheduler

import doobie.hikari.HikariTransactor
import etlflow.scheduler.api.EtlFlowHelper._
import etlflow.scheduler.db.{Query, Update}
import etlflow.utils.Executor._
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import eu.timepit.fs2cron.schedule
import fs2.Stream
import org.slf4j.{Logger, LoggerFactory}
import zio._
import zio.clock.Clock
import zio.duration._
import zio.interop.catz._
import zio.interop.catz.implicits._
import scala.reflect.runtime.universe.TypeTag

abstract class Scheduler[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag]  {

  lazy val scheduler_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val etl_job_name_package: String

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

  final def runActiveEtlJobDataProc(args: EtlJobArgs, transactor: HikariTransactor[Task], config: DATAPROC, sem: Semaphore): Task[Unit] = {
    for {
      _  <- UIO(scheduler_logger.info(s"Checking if job  ${args.name} is active at ${UF.getCurrentTimestampAsString()}"))
      cj <- Query.getCronJobFromDB(args.name,transactor)
      _  <- if (cj.is_active) UIO(s"Running job ${cj.job_name} with schedule ${cj.schedule} at ${UF.getCurrentTimestampAsString()}") *> runEtlJobDataProc(args,transactor,config,sem)
      else UIO(
        scheduler_logger.info(
          s"Skipping inactive cron job ${cj.job_name} with schedule " +
            s"${cj.schedule} at ${UF.getCurrentTimestampAsString()}"
        )
      )
    } yield ()
  }

  final def runActiveEtlJobKubernetes(args: EtlJobArgs, transactor: HikariTransactor[Task], config: KUBERNETES, sem: Semaphore): Task[Unit] = {
    for {
      _  <- UIO(scheduler_logger.info(s"Checking if job  ${args.name} is active at ${UF.getCurrentTimestampAsString()}"))
      cj <- Query.getCronJobFromDB(args.name,transactor)
      _  <- if (cj.is_active) UIO(s"Running job ${cj.job_name} with schedule ${cj.schedule} at ${UF.getCurrentTimestampAsString()}") *> runEtlJobKubernetes(args,transactor,config,sem)
      else UIO(
        scheduler_logger.info(
          s"Skipping inactive cron job ${cj.job_name} with schedule " +
            s"${cj.schedule} at ${UF.getCurrentTimestampAsString()}"
        )
      )
    } yield ()
  }

  final def runActiveEtlJobLocal(args: EtlJobArgs, transactor: HikariTransactor[Task], sem: Semaphore): Task[Unit] = {
    for {
      _  <- UIO(scheduler_logger.info(s"Checking if job  ${args.name} is active at ${UF.getCurrentTimestampAsString()}"))
      cj <- Query.getCronJobFromDB(args.name,transactor)
      _  <- if (cj.is_active) UIO(s"Running job ${cj.job_name} with schedule ${cj.schedule} at ${UF.getCurrentTimestampAsString()}") *> runEtlJobLocal(args,transactor,sem)
      else UIO(
        scheduler_logger.info(
          s"Skipping inactive cron job ${cj.job_name} with schedule " +
            s"${cj.schedule} at ${UF.getCurrentTimestampAsString()}"
        )
      )
    } yield ()
  }

  final def runActiveEtlJobLocalSubProcess(args: EtlJobArgs, transactor: HikariTransactor[Task],config: LOCAL_SUBPROCESS, sem: Semaphore): Task[Unit] = {
    for {
      _  <- UIO(scheduler_logger.info(s"Checking if job  ${args.name} is active at ${UF.getCurrentTimestampAsString()}"))
      cj <- Query.getCronJobFromDB(args.name,transactor)
      _  <- if (cj.is_active) UIO(s"Running job ${cj.job_name} with schedule ${cj.schedule} at ${UF.getCurrentTimestampAsString()}") *> runEtlJobLocalSubProcess(args,transactor,config,sem)
      else UIO(
        scheduler_logger.info(
          s"Skipping inactive cron job ${cj.job_name} with schedule " +
            s"${cj.schedule} at ${UF.getCurrentTimestampAsString()}"
        )
      )
    } yield ()
  }

  final def scheduledTask(dbCronJobs: List[CronJob], transactor: HikariTransactor[Task], jobSemaphores: Map[String, Semaphore]): Task[Unit] = {
    val jobsToBeScheduled = dbCronJobs.flatMap{ cj =>
      if (cj.schedule.isDefined)
        List(cj)
      else
        List.empty
    }
    if (jobsToBeScheduled.isEmpty) {
      scheduler_logger.warn("No scheduled jobs found")
      ZIO.unit
    }
    else {
      val cronSchedule = schedule(jobsToBeScheduled.map(cj => (cj.schedule.get,Stream.eval {
        val job_deploy_mode = UF.getEtlJobName[EJN](cj.job_name, etl_job_name_package).getActualProperties(Map.empty).job_deploy_mode

        job_deploy_mode match {
          case DATAPROC(project, region, endpoint, cluster_name) =>
            scheduler_logger.info(
              s"Scheduled cron job ${cj.job_name} with schedule ${cj.schedule.get.toString} " +
                s"at ${UF.getCurrentTimestampAsString()} in DATAPROC mode "
            )
            runActiveEtlJobDataProc(EtlJobArgs(cj.job_name, List.empty), transactor, DATAPROC(project,region,endpoint,cluster_name),jobSemaphores(cj.job_name))
          case LOCAL_SUBPROCESS(script_path, heap_min_memory, heap_max_memory) =>
            scheduler_logger.info(
              s"Scheduled cron job ${cj.job_name} with schedule ${cj.schedule.get.toString} " +
                s"at ${UF.getCurrentTimestampAsString()} in LOCAL_SUBPROCESS mode "
            )
            runActiveEtlJobLocalSubProcess(EtlJobArgs(cj.job_name, List.empty), transactor, LOCAL_SUBPROCESS(script_path, heap_min_memory, heap_max_memory),jobSemaphores(cj.job_name))
          case LOCAL =>
            scheduler_logger.info(
              s"Scheduled cron job ${cj.job_name} with schedule ${cj.schedule.get.toString} " +
                s"at ${UF.getCurrentTimestampAsString()} in LOCAL mode "
            )
            runActiveEtlJobLocal(EtlJobArgs(cj.job_name, List.empty), transactor, jobSemaphores(cj.job_name))
          case LIVY(_) =>
            scheduler_logger.warn(s"Deploy mode livy not supported " +
              s"for job ${cj.job_name}, supported values are LOCAL, DATAPROC(..)")
            ZIO.unit
          case KUBERNETES(imageName, nameSpace, envVar, containerName, entryPoint, restartPolicy) =>
            scheduler_logger.info(
              s"Scheduled cron job ${cj.job_name} with schedule ${cj.schedule.get.toString} " +
                s"at ${UF.getCurrentTimestampAsString()} in KUBERNETES mode "
            )
            runActiveEtlJobKubernetes(EtlJobArgs(cj.job_name, List.empty), transactor, KUBERNETES(imageName, nameSpace, envVar, containerName, entryPoint, restartPolicy) ,jobSemaphores(cj.job_name))
        }
      })))

      UIO(scheduler_logger.info("*"*30 + s" Scheduler heartbeat at ${UF.getCurrentTimestampAsString()} " + "*"*30))
        .repeat(Schedule.forever && Schedule.spaced(60.minute))
        .provideLayer(Clock.live).fork *> cronSchedule.compile.drain
        .mapError{e =>
          scheduler_logger.error("*"*30 + s" Scheduler crashed due to error ${e.getMessage} stacktrace ${e.printStackTrace()}" + "*"*30)
          e
        }
    }
  }

  def runEtlJobDataProc(args: EtlJobArgs, transactor: HikariTransactor[Task], config: DATAPROC, sem: Semaphore): Task[EtlJob]
  def runEtlJobLocal(args: EtlJobArgs, transactor: HikariTransactor[Task], sem: Semaphore): Task[EtlJob]
  def runEtlJobLocalSubProcess(args: EtlJobArgs, transactor: HikariTransactor[Task],config: LOCAL_SUBPROCESS, sem: Semaphore): Task[EtlJob]
  def runEtlJobKubernetes(args: EtlJobArgs, transactor: HikariTransactor[Task], config: KUBERNETES, sem: Semaphore): Task[EtlJob]

  def etlFlowScheduler(
                        transactor: HikariTransactor[Task],
                        cronJobs: Ref[List[CronJob]],
                        jobSemaphores: Map[String, Semaphore]
                      ): Task[Unit] = for {
    _          <- Update.deleteCronJobsDB(transactor,UF.getEtlJobs[EJN].map(x => x).toList)
    dbCronJobs <- refreshCronJobsDB(transactor)
    _          <- cronJobs.update{_ => dbCronJobs.filter(_.schedule.isDefined)}
    _          <- UIO(scheduler_logger.info(s"Refreshed jobs in database \n${dbCronJobs.mkString("\n")}"))
    _          <- UIO(scheduler_logger.info("Starting scheduler"))
    _          <- scheduledTask(dbCronJobs,transactor,jobSemaphores)
  } yield ()
}
