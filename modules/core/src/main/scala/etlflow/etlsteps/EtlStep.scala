package etlflow.etlsteps

import etlflow.core.{CoreEnv, CoreLogEnv}
import etlflow.log.LogWrapperApi
import etlflow.schema.LoggingLevel
import etlflow.utils.ApplicationLogger
import zio.{RIO, Task}

trait EtlStep[IPSTATE,OPSTATE] extends ApplicationLogger { self =>

  val name: String
  val step_type: String = this.getClass.getSimpleName

  def process(input_state: =>IPSTATE): RIO[CoreEnv, OPSTATE]
  def getExecutionMetrics: Map[String,Map[String,String]] = Map()
  def getStepProperties(level:LoggingLevel = LoggingLevel.INFO): Map[String,String] = Map()

  final def execute(input_state: =>IPSTATE): RIO[CoreLogEnv, OPSTATE] = {
    for {
      step_start_time <- Task.succeed(System.currentTimeMillis())
      _   <- LogWrapperApi.stepLogStart(step_start_time, self)
      op  <- process(input_state).tapError{ex =>
        LogWrapperApi.stepLogEnd(step_start_time,self,Some(ex))
      }
      _   <- LogWrapperApi.stepLogEnd(step_start_time,self)
    } yield op
  }
}