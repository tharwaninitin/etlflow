package etlflow

import etlflow.audit.{AuditApi, AuditEnv}
import etlflow.utils.{DateTimeApi, MapToJson}
import zio._
//import zio.logging.LogFormat
//import zio.logging.backend.SLF4J

trait JobApp extends ZIOAppDefault {

  def job(args: Chunk[String]): RIO[AuditEnv, Unit]

  val logLayer: ZLayer[Any, Throwable, AuditEnv] = audit.noLog

  // override val bootstrap = SLF4J.slf4j(LogLevel.Info, LogFormat.colored)

  val name: String = this.getClass.getSimpleName.replace('$', ' ').trim

  final def execute(cliArgs: Chunk[String]): RIO[AuditEnv, Unit] =
    for {
      args <- ZIO.succeed(MapToJson(cliArgs.zipWithIndex.map(t => (t._2.toString, t._1)).toMap))
      _    <- AuditApi.logJobStart(name, args, DateTimeApi.getCurrentTimestamp)
      _ <- job(cliArgs).tapError { ex =>
        AuditApi.logJobEnd(name, args, DateTimeApi.getCurrentTimestamp, Some(ex))
      }
      _ <- AuditApi.logJobEnd(name, args, DateTimeApi.getCurrentTimestamp)
    } yield ()

  override def run: ZIO[ZIOAppArgs with Scope, Any, Any] = for {
    arguments <- getArgs
    _         <- execute(arguments).provideLayer(logLayer)
  } yield ()
}
