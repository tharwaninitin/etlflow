package etlflow.task

import etlflow.log.{LogApi, LogEnv, LogEnvTry}
import etlflow.utils.{ApplicationLogger, DateTimeApi}
import zio.{RIO, ZIO}
import scala.util.{Failure, Try}

trait EtlTask[R, OP] extends ApplicationLogger {
  val name: String
  val taskType: String = this.getClass.getSimpleName

  def getExecutionMetrics: Map[String, String] = Map.empty[String, String]
  def getTaskProperties: Map[String, String]   = Map.empty[String, String]

  protected def process: RIO[R, OP]

  final def execute: RIO[R with LogEnv, OP] = for {
    sri <- ZIO.succeed(java.util.UUID.randomUUID.toString)
    _   <- LogApi.logTaskStart(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp)
    op <- process.tapError { ex =>
      LogApi.logTaskEnd(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp, Some(ex))
    }
    _ <- LogApi.logTaskEnd(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp, None)
  } yield op

  protected def processTry: Try[OP] = ???

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  final def executeTry(log: LogEnvTry): Try[OP] = for {
    sri <- Try(java.util.UUID.randomUUID.toString)
    _   <- log.logTaskStart(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp)
    op <- processTry.recoverWith { case e: Throwable =>
      log.logTaskEnd(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp, Some(e))
      Failure(e)
    }
    _ <- log.logTaskEnd(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp, None)
  } yield op
}
