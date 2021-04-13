package etlflow.scheduler

import cron4s.CronExpr
import doobie.hikari.HikariTransactor
import etlflow.utils.EtlFlowHelper._
import etlflow.utils.{EtlFlowUtils, UtilityFunctions => UF}
import etlflow.webserver.api.ApiService
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._

trait Scheduler extends EtlFlowUtils {
  final def scheduleJobs(dbCronJobs: List[CronJob]): RIO[GQLEnv with Blocking with Clock,Unit] = {
    if (dbCronJobs.isEmpty) {
      logger.warn("No scheduled jobs found")
      ZIO.unit
    }
    else {
      val listOfCron: List[(CronExpr, URIO[GQLEnv with Blocking with Clock, Option[EtlJob]])] = dbCronJobs.map(cj => (cj.schedule.get, {
        logger.info(s"Scheduling job ${cj.job_name} with schedule ${cj.schedule.get.toString} at ${UF.getCurrentTimestampAsString()}")
        ApiService
          .runJob(EtlJobArgs(cj.job_name),"Scheduler")
          .map(Some(_))
          .catchAll(_ => UIO.succeed(None))
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
  final def etlFlowScheduler(transactor: HikariTransactor[Task], cronJobs: Ref[List[CronJob]], jobs: List[EtlJob]): RIO[GQLEnv with Blocking with Clock, Unit] = for {
    dbJobs     <- refreshJobsDB(transactor,jobs)
    _          <- cronJobs.update{_ => dbJobs.filter(_.schedule.isDefined)}
    cronJobs   <- cronJobs.get
    _          <- UIO(logger.info(s"Refreshed jobs in database \n${dbJobs.mkString("\n")}"))
    _          <- UIO(logger.info("Starting scheduler"))
    _          <- scheduleJobs(cronJobs)
  } yield ()
}
