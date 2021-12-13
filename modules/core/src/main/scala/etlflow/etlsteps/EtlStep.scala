package etlflow.etlsteps

import etlflow.core.{CoreEnv, CoreLogEnv}
import etlflow.log.LogApi
import etlflow.utils.ApplicationLogger
import etlflow.utils.DateTimeApi
import zio.{RIO, UIO}

trait EtlStep[IPSTATE,OPSTATE] extends ApplicationLogger { self =>

  val name: String
  val step_type: String = this.getClass.getSimpleName

  def process(input_state: =>IPSTATE): RIO[CoreEnv, OPSTATE]
  def getExecutionMetrics: Map[String,Map[String,String]] = Map()
  def getStepProperties: Map[String,String] = Map()

  final def execute(input_state: =>IPSTATE): RIO[CoreLogEnv, OPSTATE] = {
    for {
      sri <- UIO(java.util.UUID.randomUUID.toString)
      _   <- LogApi.logStepStart(sri, name, getStepProperties, step_type, DateTimeApi.getCurrentTimestamp)
      op  <- process(input_state).tapError{ex =>
                LogApi.logStepEnd(sri, name, getStepProperties, step_type, DateTimeApi.getCurrentTimestamp, Some(ex))
             }
      _   <- LogApi.logStepEnd(sri, name, getStepProperties, step_type, DateTimeApi.getCurrentTimestamp)
    } yield op
  }
}