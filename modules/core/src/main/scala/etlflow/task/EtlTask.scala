package etlflow.task

import etlflow.audit.Audit
import etlflow.log.ApplicationLogger
import etlflow.utils.DateTimeApi
import zio.{RIO, ZIO}

trait EtlTask[R, OP] extends ApplicationLogger {
  val name: String
  val taskType: String = this.getClass.getSimpleName

  def getTaskProperties: Map[String, String] = Map.empty[String, String]

  protected def process: RIO[R, OP]

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  final def execute: RIO[R with Audit, OP] = for {
    sri <- ZIO.succeed(java.util.UUID.randomUUID.toString)
    _   <- Audit.logTaskStart(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp)
    op <- process.tapError { ex =>
      Audit.logTaskEnd(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp, Some(ex))
    }
    _ <- Audit.logTaskEnd(sri, name, getTaskProperties, taskType, DateTimeApi.getCurrentTimestamp, None)
  } yield op
}
