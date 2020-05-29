package etlflow.etlsteps

import etlflow.LoggerResource
import org.slf4j.{Logger, LoggerFactory}
import zio.{Task, ZIO}

trait EtlStep[IPSTATE,OPSTATE] { self =>
  val name: String
  val etl_logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def process(input_state: =>IPSTATE): Task[OPSTATE]
  def getExecutionMetrics: Map[String,Map[String,String]] = Map()
  def getStepProperties(level: String = "info"): Map[String,String] = Map()

  final def logStepInit(step_start_time: Long)(resource: LoggerResource): Task[Long] = {
    if (resource.db.isDefined)
      resource.db.get.updateStepLevelInformation(step_start_time, self, "started", mode = "insert")
    else ZIO.succeed(0)
  }
  final def logStepSuccess(step_start_time: Long)(resource: LoggerResource): Task[Long] = {
    resource.slack.foreach(_.updateStepLevelInformation(step_start_time, this, "pass"))
    if (resource.db.isDefined)
      resource.db.get.updateStepLevelInformation(step_start_time, this, "pass")
    else
      ZIO.succeed(0)
  }
  final def logStepError(step_start_time: Long, ex: Throwable)(resource: LoggerResource): Task[Long] = {
    resource.slack.foreach(_.updateStepLevelInformation(step_start_time, this, "failed", Some(ex.getMessage)))
    if (resource.db.isDefined) {
      etl_logger.error("Step Error StackTrace:"+"\n"+ex.getStackTrace.mkString("\n"))
      resource.db.get.updateStepLevelInformation(step_start_time, this, "failed", Some(ex.getMessage)) *> Task.fail(new RuntimeException(ex.getMessage)).as(1)
    }
    else
      Task.fail(ex)
  }

  final def execute(input_state: =>IPSTATE)(implicit resource: LoggerResource): Task[Unit] = {
    val step = for {
      step_start_time <- Task.succeed(System.currentTimeMillis())
      _ <- logStepInit(step_start_time)(resource)
      _ <- process(input_state) foldM (
              ex => logStepError(step_start_time, ex)(resource),
              _  => logStepSuccess(step_start_time)(resource)
           )
    } yield ()
    step
  }
}
