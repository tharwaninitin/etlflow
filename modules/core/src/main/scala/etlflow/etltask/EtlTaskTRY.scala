package etlflow.etltask

import etlflow.log.LogEnvTry
import etlflow.utils.DateTimeApi
import scala.util.{Failure, Try}

trait EtlTaskTRY[OP] extends EtlTask {

  protected def processTry: Try[OP]

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  final def executeTry(log: LogEnvTry): Try[OP] = for {
    sri <- Try(java.util.UUID.randomUUID.toString)
    _   <- log.logStepStart(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp)
    op <- processTry.recoverWith { case e: Throwable =>
      log.logStepEnd(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp, Some(e))
      Failure(e)
    }
    _ <- log.logStepEnd(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp, None)
  } yield op
}
