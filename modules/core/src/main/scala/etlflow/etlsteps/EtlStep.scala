package etlflow.etlsteps

import etlflow.LoggerResource
import org.slf4j.{Logger, LoggerFactory}
import zio.{Has, Task, ZIO, ZLayer}

trait EtlStep[IPSTATE,OPSTATE] { self =>
  val etl_logger: Logger = LoggerFactory.getLogger(getClass.getName)
  import EtlStep._

  val name: String

  def process(input_state: =>IPSTATE): Task[OPSTATE]
  def getExecutionMetrics: Map[String,Map[String,String]] = Map()
  def getStepProperties(level: String = "info"): Map[String,String] = Map()

  final def execute(input_state: =>IPSTATE): ZIO[LoggerResource, Throwable, OPSTATE] = {
    val env = LoggerResourceClient.live >>> StepLogger.live(self)
    val step = for {
      step_start_time <- Task.succeed(System.currentTimeMillis())
      _ <- StepLogger.logStepInit(step_start_time)
      op <- process(input_state).tapError{ex =>
              StepLogger.logStepError(step_start_time, ex)
            }
      _ <- StepLogger.logStepSuccess(step_start_time)
    } yield op
    step.provideLayer(env)
  }
}

object EtlStep {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  type LoggerResourceClient = Has[LoggerResourceClient.Service]
  object LoggerResourceClient {
    trait Service {
      val loggerResource: LoggerResource
    }
    val live: ZLayer[LoggerResource, Nothing, LoggerResourceClient] =
      ZLayer.fromFunction((curr: LoggerResource) => new LoggerResourceClient.Service { val loggerResource: LoggerResource = curr })
  }

  type StepLogger = Has[StepLogger.Service]
  object StepLogger {
    trait Service {
      def logStepInit(step_start_time: Long): Task[Long]
      def logStepSuccess(step_start_time: Long): Task[Long]
      def logStepError(step_start_time: Long, ex: Throwable): Task[Long]
    }
    def live[IP,OP](etlStep: EtlStep[IP,OP]): ZLayer[LoggerResourceClient, Throwable, StepLogger] = ZLayer.fromService { deps: LoggerResourceClient.Service =>
      new StepLogger.Service {
        def logStepInit(step_start_time: Long): Task[Long] = {
          if (deps.loggerResource.db.isDefined)
            deps.loggerResource.db.get.updateStepLevelInformation(step_start_time, etlStep, "started", mode = "insert")
          else ZIO.succeed(0)
        }
        def logStepSuccess(step_start_time: Long): Task[Long] = {
          deps.loggerResource.slack.foreach(_.updateStepLevelInformation(step_start_time, etlStep, "pass"))
          if (deps.loggerResource.db.isDefined)
            deps.loggerResource.db.get.updateStepLevelInformation(step_start_time, etlStep, "pass")
          else
            ZIO.succeed(0)
        }
        def logStepError(step_start_time: Long, ex: Throwable): Task[Long] = {
          deps.loggerResource.slack.foreach(_.updateStepLevelInformation(step_start_time, etlStep, "failed", Some(ex.getMessage)))
          if (deps.loggerResource.db.isDefined) {
            logger.error("Step Error StackTrace:"+"\n"+ex.getStackTrace.mkString("\n"))
            deps.loggerResource.db.get.updateStepLevelInformation(step_start_time, etlStep, "failed", Some(ex.getMessage)) *> Task.fail(new RuntimeException(ex.getMessage)).as(1)
          }
          else
            Task.fail(ex)
        }
      }
    }

    def logStepInit(step_start_time: Long): ZIO[StepLogger, Throwable, Long] = ZIO.accessM(_.get.logStepInit(step_start_time))
    def logStepSuccess(step_start_time: Long): ZIO[StepLogger, Throwable, Long] = ZIO.accessM(_.get.logStepSuccess(step_start_time))
    def logStepError(step_start_time: Long, ex: Throwable): ZIO[StepLogger, Throwable, Long] = ZIO.accessM(_.get.logStepError(step_start_time,ex))
  }
}