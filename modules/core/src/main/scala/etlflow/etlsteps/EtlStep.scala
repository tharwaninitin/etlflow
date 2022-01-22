package etlflow.etlsteps

import etlflow.core.{CoreEnv, CoreLogEnv}
import etlflow.log.LogApi
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi
import zio.{RIO, UIO}

trait EtlStep[OP] extends ApplicationLogger {

  val name: String
  val step_type: String = this.getClass.getSimpleName

  def process: RIO[CoreEnv, OP]
  def getExecutionMetrics: Map[String, String] = Map()
  def getStepProperties: Map[String, String]   = Map()

  final def execute: RIO[CoreLogEnv, OP] =
    for {
      sri <- UIO(java.util.UUID.randomUUID.toString)
      _   <- LogApi.logStepStart(sri, name, getStepProperties, step_type, DateTimeApi.getCurrentTimestamp)
      op <- process.tapError { ex =>
        LogApi.logStepEnd(sri, name, getStepProperties, step_type, DateTimeApi.getCurrentTimestamp, Some(ex))
      }
      _ <- LogApi.logStepEnd(sri, name, getStepProperties, step_type, DateTimeApi.getCurrentTimestamp)
    } yield op
}

object EtlStep {
  /* Features to develop
    SEMAPHORE - Control at global level total number of parallel steps execution
   */
}
