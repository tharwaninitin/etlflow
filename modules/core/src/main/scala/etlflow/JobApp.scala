package etlflow

import etlflow.audit.{AuditApi, AuditEnv}
import etlflow.log.ApplicationLogger
import etlflow.utils.{DateTimeApi, MapToJson}
import zio._

/** This is the entry point for a EtlFlow Job application (See below sample).
  *
  * {{{
  * import etlflow._
  * import etlflow.task._
  * import zio._
  *
  * object MyJobApp extends JobApp {
  *
  *   def executeTask(): Unit = logger.info(s"Hello EtlFlow Task")
  *
  *   val task1: GenericTask[Unit] = GenericTask(
  *       name = "Task_1",
  *       function = executeTask()
  *   )
  *
  *   def job(args: Chunk[String]): RIO[audit.AuditEnv, Unit] = task1.execute
  * }
  * }}}
  */
trait JobApp extends ZIOAppDefault with ApplicationLogger {

  def job(args: Chunk[String]): RIO[AuditEnv, Unit]

  val auditLayer: ZLayer[Any, Throwable, AuditEnv] = audit.noLog

  override val bootstrap = zioSlf4jLogger

  val name: String = this.getClass.getSimpleName.replace('$', ' ').trim

  /** This is the core function which runs the job with auditing (start and end).
    *
    * It also converts command-line arguments passed to application into json key value pair. For e.g. if you run application with
    * args "arg0 arg1 arg2 arg3" it will parse and convert these args to "{"0":"arg0", "1":"arg1", "2":"arg2", "3":"arg3"}"
    *
    * @param cliArgs
    *   command-line arguments
    */
  final def execute(cliArgs: Chunk[String]): RIO[AuditEnv, Unit] =
    for {
      args <- ZIO.succeed(MapToJson(cliArgs.zipWithIndex.map(t => (t._2.toString, t._1)).toMap))
      _    <- AuditApi.logJobStart(name, args, DateTimeApi.getCurrentTimestamp)
      _ <- job(cliArgs).tapError { ex =>
        AuditApi.logJobEnd(name, args, DateTimeApi.getCurrentTimestamp, Some(ex))
      }
      _ <- AuditApi.logJobEnd(name, args, DateTimeApi.getCurrentTimestamp)
    } yield ()

  /** This is just a wrapper around default run method available with ZIOAppDefault to call [[execute execute(Chunk[String])]]
    */
  final override def run: ZIO[ZIOAppArgs with Scope, Any, Any] = for {
    arguments <- getArgs
    _         <- execute(arguments).provideLayer(auditLayer)
  } yield ()
}
