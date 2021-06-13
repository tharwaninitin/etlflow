package etlflow.scheduler

import cron4s.{Cron, CronExpr}
import etlflow.api.Schema._
import etlflow.api.{ServerEnv, ServerTask, Service}
import etlflow.jdbc.DB
import etlflow.log.ApplicationLogger
import etlflow.schema.EtlJob
import etlflow.utils.{UtilityFunctions => UF}
import zio._
import zio.duration._

trait Scheduler extends ApplicationLogger {
  case class CronJob(job_name: String, schedule: Option[CronExpr])
  final def scheduleJobs(dbCronJobs: List[CronJob]): ServerTask[Unit] = {
    if (dbCronJobs.isEmpty) {
      logger.warn("No scheduled jobs found")
      ZIO.unit
    }
    else {
      val listOfCron: List[(String, CronExpr, URIO[ServerEnv, Option[EtlJob]])] = dbCronJobs.map(cj => (cj.job_name,cj.schedule.get, {
        logger.info(s"Scheduling job ${cj.job_name} with schedule ${cj.schedule.get.toString} at ${UF.getCurrentTimestampAsString()}")
        Service
          .runJob(EtlJobArgs(cj.job_name),"Scheduler")
          .map(Some(_))
          .catchAll(_ => UIO.none)
      }))

      val scheduledJobs = repeatEffectsForCronWithName(listOfCron)

      val scheduledLogger = UIO(logger.info("*"*30 + s" Scheduler heartbeat at ${UF.getCurrentTimestampAsString()} " + "*"*30))
        .repeat(Schedule.forever && Schedule.spaced(60.minute))

      //scheduledJobs.zipPar(scheduledLogger).unit
      scheduledJobs
        .tapError{e =>
          UIO(logger.error("*"*30 + s" Scheduler crashed due to error ${e.getMessage} stacktrace ${e.printStackTrace()}" + "*"*30))
        }
    }
  }
  final def etlFlowScheduler(jobs: List[EtlJob]): ServerTask[Unit] = for {
    dbJobs      <- DB.refreshJobs(jobs)
    cronJobs    = dbJobs.map(x => CronJob(x.job_name, Cron(x.schedule).toOption)).filter(_.schedule.isDefined)
    _           <- UIO(logger.info(s"Refreshed jobs in database \n${dbJobs.mkString("\n")}"))
    _           <- UIO(logger.info("Starting scheduler"))
    _           <- scheduleJobs(cronJobs)
  } yield ()
}
