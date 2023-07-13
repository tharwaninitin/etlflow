package etlflow.task

import etlflow.audit.Audit
import etlflow.json.JSON
import etlflow.log.ApplicationLogger
import zio.{RIO, ZIO}

/** EtlTask provides an interface that defines a unit of work that can be executed. This interface has a `toZIO` method that
  * converts the EtlTask object to a ZIO effect by wrapping the [[etlflow.audit.Audit]] API around it.
  *
  * @tparam R
  *   The ZIO environment type.
  * @tparam OP
  *   The task output type.
  */
trait EtlTask[-R, +OP] extends ApplicationLogger {

  /** The name of the task.
    */
  val name: String

  /** The task type.
    */
  val taskType: String = this.getClass.getSimpleName

  /** The abstract method that needs to be implemented by classes extending EtlTask to define the ZIO effect.
    *
    * @return
    *   The ZIO effect representing the task execution.
    */
  protected def process: RIO[R, OP]

  /** Get metadata associated with the task.
    *
    * @return
    *   A map of metadata key-value pairs.
    */
  def getMetaData: Map[String, String] = Map.empty[String, String]

  /** Convert the EtlTask to a ZIO effect, tracking the execution progress using the [[etlflow.audit.Audit]] API's task start and
    * task end methods.
    *
    * @return
    *   The ZIO effect representing the task execution with audit logging.
    */
  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  final def toZIO: RIO[R with Audit, OP] = for {
    tri   <- ZIO.succeed(java.util.UUID.randomUUID.toString)
    props <- JSON.convertToStringZIO(getMetaData)
    _     <- Audit.logTaskStart(tri, name, props, taskType)
    op    <- process.tapError(ex => Audit.logTaskEnd(tri, Some(ex)))
    _     <- Audit.logTaskEnd(tri, None)
  } yield op
}
