package etlflow.task

import etlflow.audit.Audit
import etlflow.json.JSON
import etlflow.log.ApplicationLogger
import zio.{RIO, ZIO}

trait EtlTask[-R, +OP] extends ApplicationLogger {
  val name: String
  val taskType: String = this.getClass.getSimpleName

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  final def toZIO: RIO[R with Audit, OP] = for {
    tri   <- ZIO.succeed(java.util.UUID.randomUUID.toString)
    props <- JSON.convertToStringZIO(getTaskProperties)
    _     <- Audit.logTaskStart(tri, name, props, taskType)
    op    <- process.tapError(ex => Audit.logTaskEnd(tri, Some(ex)))
    _     <- Audit.logTaskEnd(tri, None)
  } yield op

  def getTaskProperties: Map[String, String] = Map.empty[String, String]

  protected def process: RIO[R, OP]

}
