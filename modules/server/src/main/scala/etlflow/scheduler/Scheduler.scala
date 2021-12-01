package etlflow.scheduler

import com.cronutils.model.time.ExecutionTime
import etlflow.api.Schema._
import etlflow.api.{ServerEnv, ServerTask, Service}
import etlflow.db.{DBServerApi, EtlJob}
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.getCurrentTimestampAsString
import zio._
import zio.duration._

private [etlflow] trait Scheduler extends ApplicationLogger {
  
  final def scheduleJobs(dbCronJobs: List[CronJob]): ServerTask[Unit] = {
    if (dbCronJobs.isEmpty) {
      logger.warn("No scheduled jobs found")
      ZIO.unit
    }
    else {
      val listOfCron: List[(String, ExecutionTime, URIO[ServerEnv, Option[EtlJob]])] = dbCronJobs.map(cj => (cj.job_name,cj.schedule.cron.get, {
        logger.info(s"Scheduling job ${cj.job_name} with schedule ${cj.schedule.name} at ${getCurrentTimestampAsString()}")
        Service
          .runJob(EtlJobArgs(cj.job_name),"Scheduler")
          .map(Some(_))
          .catchAll(_ => UIO.none)
      }))

      val scheduledJobs = repeatEffectsForCronWithName(listOfCron)

      UIO(logger.info("*"*30 + s" Scheduler heartbeat at ${getCurrentTimestampAsString()} " + "*"*30))
        .repeat(Schedule.forever && Schedule.spaced(60.minute))

      //scheduledJobs.zipPar(scheduledLogger).unit
      scheduledJobs
        .tapError{e =>
          UIO(logger.error("*"*30 + s" Scheduler crashed due to error ${e.getMessage} stacktrace ${e.printStackTrace()}" + "*"*30))
        }
    }
  }
  final def etlFlowScheduler(jobs: List[EtlJob]): ServerTask[Unit] = for {
    dbJobs      <- DBServerApi.refreshJobs(jobs)
    cronJobs    =  dbJobs.map(x => CronJob(x.job_name, Cron(x.schedule))).filter(_.schedule.cron.isDefined)
    _           <- UIO(logger.info(s"Refreshed jobs in database \n${dbJobs.mkString("\n")}"))
    _           <- UIO(logger.info("Starting scheduler"))
    _           <- scheduleJobs(cronJobs)
  } yield ()
}
