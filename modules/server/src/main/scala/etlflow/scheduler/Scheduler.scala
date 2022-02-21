package etlflow.scheduler

import com.cronutils.model.Cron
import cron4zio._
import etlflow.server.{ServerEnv, ServerTask}
import etlflow.server.model.EtlJob
import etlflow.server.{DBServerApi, Service}
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi.getCurrentTimestampAsString
import zio._

object Scheduler extends ApplicationLogger {

  final def scheduleJobs(dbCronJobs: List[CronJob]): ServerTask[Unit] =
    if (dbCronJobs.isEmpty) {
      logger.warn("No scheduled jobs found")
      ZIO.unit
    } else {
      val listOfCron: List[(URIO[ServerEnv, Option[EtlJob]], Cron)] = dbCronJobs.map(cj =>
        (
          {
            logger.info(
              s"Scheduling job ${cj.name} with schedule ${cj.schedule.get.asString()} at ${getCurrentTimestampAsString()}"
            )
            Service
              .runJob(cj.name, Map.empty, "Scheduler")
              .map(Some(_))
              .catchAll(_ => UIO.none)
          },
          cj.schedule.get
        )
      )

      repeatEffectsForCron(listOfCron).unit
        .tapError { e =>
          UIO(
            logger.error(
              "*" * 30 + s" Scheduler crashed due to error ${e.getMessage} stacktrace ${e.printStackTrace()}" + "*" * 30
            )
          )
        }
    }

  final def apply(jobs: List[EtlJob]): ServerTask[Unit] = for {
    dbJobs <- DBServerApi.refreshJobs(jobs)
    cronJobs = dbJobs.map(x => new CronJob(x.job_name, parse(x.schedule).toOption)).filter(_.schedule.isDefined)
    _ <- UIO(logger.info(s"Refreshed jobs in database \n${dbJobs.mkString("\n")}"))
    _ <- UIO(logger.info("Starting scheduler"))
    _ <- scheduleJobs(cronJobs)
  } yield ()
}
