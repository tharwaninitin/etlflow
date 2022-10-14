package etlflow.task

import etlflow.audit.{AuditApi, AuditEnv}
import etlflow.log.ApplicationLogger
import etlflow.utils.DateTimeApi
import zio.{RIO, ZIO}

trait EtlTask[R, OP] extends ApplicationLogger {
  val name: String
  val taskType: String = this.getClass.getSimpleName

  def getTaskProperties: Map[String, String] = Map.empty[String, String]

  protected def process: RIO[R, OP]

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  final def execute: RIO[R with AuditEnv, OP] = for {
    sri <- ZIO.succeed(java.util.UUID.randomUUID.toString)
    _   <- AuditApi.logTaskStart(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp)
    op <- process.tapError { ex =>
      AuditApi.logTaskEnd(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp, Some(ex))
    }
    _ <- AuditApi.logTaskEnd(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp, None)
  } yield op
}
