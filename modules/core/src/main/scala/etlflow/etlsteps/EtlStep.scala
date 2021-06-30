package etlflow.etlsteps

import etlflow.log.StepLogger._
import etlflow.log.StepReq
import etlflow.schema.LoggingLevel
import etlflow.utils.ApplicationLogger
import etlflow.{JobEnv, StepEnv}
import zio.{Has, RIO, Task, ZIO, ZLayer}

trait EtlStep[IPSTATE,OPSTATE] extends ApplicationLogger { self =>

  val name: String
  val step_type: String = this.getClass.getSimpleName

  def process(input_state: =>IPSTATE): RIO[JobEnv, OPSTATE]
  def getExecutionMetrics: Map[String,Map[String,String]] = Map()
  def getStepProperties(level:LoggingLevel  = LoggingLevel.INFO): Map[String,String] = Map()

  final def execute(input_state: =>IPSTATE): ZIO[StepEnv, Throwable, OPSTATE] = {
    val env: ZLayer[Has[StepReq], Throwable, LoggingSupport] = StepLoggerResourceEnv.live >>> StepLoggerImpl.live(self)
    val step: ZIO[LoggingSupport with JobEnv, Throwable, OPSTATE] = for {
      step_start_time <- Task.succeed(System.currentTimeMillis())
      _   <- logInit(step_start_time)
      op  <- process(input_state).tapError{ex =>
               logError(step_start_time, ex)
             }
      _   <- logSuccess(step_start_time)
    } yield op
    step.provideSomeLayer[Has[StepReq] with JobEnv](env)
  }
}