package etlflow.task

import etlflow.log.LogEnvTry
import etlflow.utils.DateTimeApi
import scala.util.{Failure, Try}

trait EtlTaskTRY[OP] extends EtlTask {

  protected def processTry: Try[OP]

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
