package etlflow.task

import etlflow.audit.Audit
import etlflow.json.JSON
import etlflow.log.ApplicationLogger
import zio.{RIO, ZIO}

/** @tparam R
  *   ZIO Environment Type
  * @tparam OP
  *   Task Output Type A
  *
  * EtlTask provides interface which defines a unit of Task that can be executed. This interface has a toZIO method that converts
  * the EtlTask object to ZIO effect by wrapping [[etlflow.audit.Audit]] API around it.
  */
trait EtlTask[-R, +OP] extends ApplicationLogger {
  val name: String
  val taskType: String = this.getClass.getSimpleName

  /** @return
    *   RIO[R, OP]
    *
    * This is the abstract method which has to be implemented by Class extending EtlTask to define the ZIO effect.
    */
  protected def process: RIO[R, OP]

  /** @return
    *   Map[String, String]
    *
    * This is the abstract method which has to be implemented by Class extending EtlTask to define Metadata.
    */
  def getMetaData: Map[String, String] = Map.empty[String, String]

  /** @return
    *   RIO[R with Audit, OP]
    *
    * This method uses [[etlflow.audit.Audit]] API task start and task end methods to track execution progress of EtlTask.
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
