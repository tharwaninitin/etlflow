package etlflow

import etlflow.audit.{LogApi, LogEnv}
import etlflow.utils.{DateTimeApi, MapToJson}
import zio._
//import zio.logging.LogFormat
//import zio.logging.backend.SLF4J

trait JobApp extends ZIOAppDefault {

  def job(args: Chunk[String]): RIO[LogEnv, Unit]

  val logLayer: ZLayer[Any, Throwable, LogEnv] = audit.noLog

  // override val bootstrap = SLF4J.slf4j(LogLevel.Info, LogFormat.colored)

  val name: String = this.getClass.getSimpleName.replace('$', ' ').trim

  final def execute(cliArgs: Chunk[String]): RIO[LogEnv, Unit] =
    for {
      args <- ZIO.succeed(MapToJson(cliArgs.zipWithIndex.map(t => (t._2.toString, t._1)).toMap))
      _    <- LogApi.logJobStart(name, args, DateTimeApi.getCurrentTimestamp)
      _ <- job(cliArgs).tapError { ex =>
        LogApi.logJobEnd(name, args, DateTimeApi.getCurrentTimestamp, Some(ex))
      }
      _ <- LogApi.logJobEnd(name, args, DateTimeApi.getCurrentTimestamp)
    } yield ()

  override def run: ZIO[ZIOAppArgs with Scope, Any, Any] = for {
    arguments <- getArgs
    _         <- execute(arguments).provideLayer(logLayer)
  } yield ()
}
