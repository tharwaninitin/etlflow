package etlflow

import etlflow.core.StepEnv
import etlflow.log._
import etlflow.utils.DateTimeApi.getCurrentTimestamp
import etlflow.utils.{ApplicationLogger, Configuration}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.{App, ExitCode, RIO, UIO, URIO, ZEnv, ZIO, ZLayer}

trait EtlFlowApp extends ApplicationLogger with App {

  def job(args: List[String]): RIO[StepEnv, Unit]

  val job_name: String = this.getClass.getSimpleName.replace('$',' ').trim

  final def execute(args: List[String]): ZIO[StepEnv, Throwable, Unit] = {
    for {
      job_start_time  <- UIO(getCurrentTimestamp)
      jri             = java.util.UUID.randomUUID.toString
      _               <- LoggerApi.setJobRunId(jri)
      _               <- LoggerApi.jobLogStart(job_start_time, "EtlFlowApp", job_name, "{}", "false")
      _               <- job(args).foldM(
        ex => LoggerApi.jobLogEnd(job_start_time, jri, job_name, Some(ex)),
        _  => LoggerApi.jobLogEnd(job_start_time, jri, job_name)
      )
    } yield ()
  }

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    Configuration.config.flatMap(cfg => {
      val dbLogLayer = if(cfg.db.isEmpty) log.DBNoLogImplementation() else log.DBLiveImplementation(cfg.db.get, job_name + "-Pool")
      val consoleLogLayer = etlflow.log.ConsoleImplementation.live
      val slackLogLayer = if(cfg.slack.isEmpty) etlflow.log.SlackImplementation.nolog else etlflow.log.SlackImplementation.live(cfg.slack)
      val logLayer = log.Implementation.live
      val fullLogLayer: ZLayer[Blocking, Throwable, DBLogEnv with LoggerEnv with ConsoleEnv with SlackEnv] =
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
