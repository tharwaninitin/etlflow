package etlflow

import etlflow.core.CoreLogEnv
import etlflow.log.{LogApi, LogEnv}
import etlflow.utils.{ApplicationLogger, DateTimeApi, MapToJson}
import zio.{App, ExitCode, RIO, UIO, URIO, ZEnv, ZIO, ZLayer}

trait JobApp extends ApplicationLogger with App {

  def job(args: List[String]): RIO[CoreLogEnv, Unit]
  val log_layer: ZLayer[ZEnv, Throwable, LogEnv] = log.nolog
  val job_name: String = this.getClass.getSimpleName.replace('$',' ').trim

  final def execute(args: List[String]): ZIO[CoreLogEnv, Throwable, Unit] = {
    for {
      job_start_time  <- UIO(DateTimeApi.getCurrentTimestamp)
      jri             = java.util.UUID.randomUUID.toString
      job_args        = MapToJson(args.zipWithIndex.map(t => (t._2.toString, t._1)).toMap)
      _               <- LogApi.setJobRunId(jri)
      _               <- LogApi.logJobStart(jri, job_name, job_args, job_start_time)
      _               <- job(args).tapError{ex =>
                           LogApi.logJobEnd(jri, job_name, job_args, DateTimeApi.getCurrentTimestamp, Some(ex))
                         }
        _             <- LogApi.logJobEnd(jri, job_name, job_args, DateTimeApi.getCurrentTimestamp)
    } yield ()
  }

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = execute(args).provideSomeLayer[ZEnv](log_layer).exitCode
}

