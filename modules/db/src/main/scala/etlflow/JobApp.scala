package etlflow

import etlflow.core.{CoreLogEnv, LogEnv}
import etlflow.log.LogWrapperApi
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import etlflow.utils.{ApplicationLogger, Configuration}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{App, ExitCode, RIO, UIO, URIO, ZEnv, ZIO, ZLayer}

trait JobApp extends ApplicationLogger with App {

  def job(args: List[String]): RIO[CoreLogEnv, Unit]

  val job_name: String = this.getClass.getSimpleName.replace('$',' ').trim

  final def execute(args: List[String]): ZIO[CoreLogEnv, Throwable, Unit] = {
    for {
      job_start_time  <- UIO(getCurrentTimestamp)
      jri             = java.util.UUID.randomUUID.toString
      _               <- LogWrapperApi.setJobRunId(jri)
      _               <- LogWrapperApi.jobLogStart(job_start_time, "EtlFlowApp", job_name, "{}", "false")
      _               <- job(args).foldM(
        ex => LogWrapperApi.jobLogEnd(job_start_time, jri, job_name, Some(ex)),
        _  => LogWrapperApi.jobLogEnd(job_start_time, jri, job_name)
      )
    } yield ()
  }

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    Configuration.config.flatMap(cfg => {
      val dbLogLayer = if(cfg.db.isEmpty) log.DBNoLogImplementation() else log.DBLiveImplementation(cfg.db.get, job_name + "-Pool")
      val consoleLogLayer = etlflow.log.ConsoleImplementation.live
      val slackLogLayer = if(cfg.slack.isEmpty) etlflow.log.SlackImplementation.nolog else etlflow.log.SlackImplementation.live(cfg.slack)
      val logLayer = log.Implementation.live
      val fullLogLayer: ZLayer[Blocking, Throwable, LogEnv] =
        logLayer ++ dbLogLayer ++ consoleLogLayer ++ slackLogLayer
      execute(args).provideSomeLayer[Blocking with Clock](fullLogLayer).catchAll{err =>
        UIO {
          logger.error(err.getMessage)
          err.getStackTrace.foreach(x => logger.error(x.toString))
        }
      }
    })
  }.exitCode
}
