package etlflow.scheduler

import cron4s.{Cron, CronExpr}
import etlflow.jdbc.{DB, DBEnv}
import etlflow.api.Schema._
import etlflow.utils.{EtlFlowUtils, UtilityFunctions => UF}
import etlflow.api.Service
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

trait Scheduler extends EtlFlowUtils {
  case class CronJob(job_name: String, schedule: Option[CronExpr])
  final def scheduleJobs(dbCronJobs: List[CronJob]): RIO[GQLEnv with DBEnv with Blocking with Clock,Unit] = {
    if (dbCronJobs.isEmpty) {
      logger.warn("No scheduled jobs found")
      ZIO.unit
    }
    else {
      val listOfCron: List[(CronExpr, URIO[GQLEnv with DBEnv with Blocking with Clock, Option[EtlJob]])] = dbCronJobs.map(cj => (cj.schedule.get, {
        logger.info(s"Scheduling job ${cj.job_name} with schedule ${cj.schedule.get.toString} at ${UF.getCurrentTimestampAsString()}")
        Service
          .runJob(EtlJobArgs(cj.job_name),"Scheduler")
          .map(Some(_))
          .catchAll(_ => UIO.none)
      }))

      val scheduledJobs = repeatEffectsForCron(listOfCron)

      val scheduledLogger = UIO(logger.info("*"*30 + s" Scheduler heartbeat at ${UF.getCurrentTimestampAsString()} " + "*"*30))
        .repeat(Schedule.forever && Schedule.spaced(60.minute))

      scheduledJobs.zipPar(scheduledLogger).unit
        .tapError{e =>
          UIO(logger.error("*"*30 + s" Scheduler crashed due to error ${e.getMessage} stacktrace ${e.printStackTrace()}" + "*"*30))
        }
    }
  }
  final def etlFlowScheduler(jobs: List[EtlJob]): RIO[GQLEnv with DBEnv with Blocking with Clock, Unit] = for {
    dbJobs      <- DB.refreshJobs(jobs)
    cronJobs    = dbJobs.map(x => CronJob(x.job_name, Cron(x.schedule).toOption)).filter(_.schedule.isDefined)
    _           <- UIO(logger.info(s"Refreshed jobs in database \n${dbJobs.mkString("\n")}"))
    _           <- UIO(logger.info("Starting scheduler"))
    _           <- scheduleJobs(cronJobs)
  } yield ()
}
