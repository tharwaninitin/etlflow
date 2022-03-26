package etlflow.etlsteps

import etlflow.log.{LogApi, LogEnv}
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi
import zio.{RIO, UIO}

trait EtlStep[R, OP] extends ApplicationLogger {
  val name: String
  val stepType: String = this.getClass.getSimpleName

  protected def process: RIO[R, OP]
  def getExecutionMetrics: Map[String, String] = Map()
  def getStepProperties: Map[String, String]   = Map()

  final def execute: RIO[R with LogEnv, OP] =
    for {
      sri <- UIO(java.util.UUID.randomUUID.toString)
      _   <- LogApi.logStepStart(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp)
      op <- process.tapError { ex =>
        LogApi.logStepEnd(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp, Some(ex))
      }
      _ <- LogApi.logStepEnd(sri, name, getStepProperties, stepType, DateTimeApi.getCurrentTimestamp)
    } yield op
}

object EtlStep {
  /* Features to develop
    SEMAPHORE - Control at global level total number of parallel steps execution
   */
}
