package etlflow.etlsteps

import etlflow.log.{LogApi, LogEnv}
import etlflow.utils.{ApplicationLogger, DateTimeApi}
import zio.{RIO, UIO}
import scala.util.{Failure, Try}

trait EtlStep[OP] extends ApplicationLogger {
  protected type R

  val name: String
  val stepType: String = this.getClass.getSimpleName

  protected def process: RIO[R, OP]
  protected def processTry: Try[OP] = ???

  def getExecutionMetrics: Map[String, String] = Map.empty[String, String]
  def getStepProperties: Map[String, String]   = Map.empty[String, String]

  final def execute: RIO[R with LogEnv, OP] = for {
    sri <- UIO(java.util.UUID.randomUUID.toString)
    _   <- LogApi.logStepStart(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp)
    op <- process.tapError { ex =>
      LogApi.logStepEnd(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp, Some(ex))
    }
    _ <- LogApi.logStepEnd(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp)
  } yield op

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  final def executeTry(log: etlflow.log.Service[Try]): Try[OP] = for {
    sri <- Try(java.util.UUID.randomUUID.toString)
    _   <- log.logStepStart(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp)
    op <- processTry.recoverWith { case e: Throwable =>
      log.logStepEnd(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp, Some(e))
      Failure(e)
    }
    _ <- log.logStepEnd(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp, None)
  } yield op
}

object EtlStep {
  /* Features to develop
    SEMAPHORE - Control at global level total number of parallel steps execution
   */
}
