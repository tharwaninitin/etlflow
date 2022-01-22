package etlflow

import etlflow.log.{LogApi, LogEnv}
import etlflow.utils.{ApplicationLogger, DateTimeApi, MapToJson}
import zio.{App, ExitCode, RIO, UIO, URIO, ZEnv, ZIO, ZLayer}

trait JobApp extends ApplicationLogger with App {

  def job(args: List[String]): RIO[ZEnv with LogEnv, Unit]

  val log_layer: ZLayer[ZEnv, Throwable, LogEnv] = log.nolog

  val name: String = this.getClass.getSimpleName.replace('$', ' ').trim

  final def execute(cli_args: List[String]): ZIO[ZEnv with LogEnv, Throwable, Unit] =
    for {
      args <- UIO(MapToJson(cli_args.zipWithIndex.map(t => (t._2.toString, t._1)).toMap))
      _    <- LogApi.logJobStart(name, args, DateTimeApi.getCurrentTimestamp)
      _ <- job(cli_args).tapError { ex =>
        LogApi.logJobEnd(name, args, DateTimeApi.getCurrentTimestamp, Some(ex))
      }
      _ <- LogApi.logJobEnd(name, args, DateTimeApi.getCurrentTimestamp)
    } yield ()

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = execute(args).provideSomeLayer[ZEnv](log_layer).exitCode
}
