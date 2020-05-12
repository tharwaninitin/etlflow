package etljobs.etlsteps

import etljobs.log.{DbManager, SlackManager}
import org.apache.log4j.Logger
import zio.{Task, ZIO}

trait EtlStep[IPSTATE,OPSTATE] { self =>
  val name: String
  val etl_logger: Logger = Logger.getLogger(getClass.getName)

  def process(input_state: IPSTATE): Task[OPSTATE]
  def getExecutionMetrics: Map[String,Map[String,String]] = Map()
  def getStepProperties(level: String = "info"): Map[String,String] = Map()

  final def logStepInit(step_start_time: Long)(db: Option[DbManager]): Task[Long] = {
    if (db.isDefined)
      db.get.updateStepLevelInformation(step_start_time, self, "started", mode = "insert")
    else ZIO.succeed(0)
  }
  final def logStepSuccess(step_start_time: Long)(db: Option[DbManager]): Task[Long] = {
    if (db.isDefined)
      db.get.updateStepLevelInformation(step_start_time, this, "pass")
    else
      ZIO.succeed(0)
  }
  final def logStepError(step_start_time: Long, ex: Throwable)(db: Option[DbManager]): Task[Long] = {
    if (db.isDefined)
      db.get.updateStepLevelInformation(step_start_time, this, "failed", Some(ex.getMessage))
    else
      ZIO.fail(ex)
  }
  final def execute(input_state: IPSTATE)(implicit db: Option[DbManager] = None, slack: Option[SlackManager] = None): Task[Unit] = {
    //    def errorHandler(exception: Throwable): Throwable = {
    //      slack.foreach(_.updateStepLevelInformation(step_start_time, this, "failed", Some(exception.getMessage)))
    //      db.foreach(_.updateStepLevelInformation(step_start_time, this, "failed", Some(exception.getMessage)))
    //      etl_logger.error(s"Error Occurred in step $name ${exception.getMessage}")
    //      exception
    //    }
    //
    //    def successHandler(): Task[Unit] = Task {
    //      slack.foreach(_.updateStepLevelInformation(step_start_time, this, "pass"))
    //      db.foreach(_.updateStepLevelInformation(step_start_time, this, "pass"))
    //    }
    for {
      step_start_time <- Task.succeed(System.currentTimeMillis())
      _ <- logStepInit(step_start_time)(db)
      _ <- process(input_state) foldM (
              ex => logStepError(step_start_time, ex)(db),
              _  => logStepSuccess(step_start_time)(db)
           )
    } yield ()
  }
}
