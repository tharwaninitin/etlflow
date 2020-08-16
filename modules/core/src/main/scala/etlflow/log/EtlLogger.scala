package etlflow.log

import etlflow.LoggerResource
import etlflow.etlsteps.EtlStep
import org.slf4j.{Logger, LoggerFactory}
import zio.{Has, Task, UIO, ZIO, ZLayer}
import etlflow.utils.{UtilityFunctions => UF}

object EtlLogger {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  type LoggerResourceClient = Has[LoggerResourceClient.Service]
  object LoggerResourceClient {
    trait Service {
      val loggerResource: LoggerResource
    }
    val live: ZLayer[LoggerResource, Nothing, LoggerResourceClient] =
      ZLayer.fromFunction((curr: LoggerResource) => new LoggerResourceClient.Service { val loggerResource: LoggerResource = curr })

    val newLive: ZLayer[Has[LoggerResource], Nothing, LoggerResourceClient] = ZLayer.fromService { deps =>
      new LoggerResourceClient.Service { val loggerResource: LoggerResource = deps }
    }
  }

  type LoggingSupport = Has[LoggerService]
  trait LoggerService {
    def logInit(start_time: Long): Task[Unit]
    def logSuccess(start_time: Long): Task[Unit]
    def logError(start_time: Long, ex: Throwable): Task[Unit]
  }
  def logInit(start_time: Long): ZIO[LoggingSupport, Throwable, Unit] = ZIO.accessM(_.get.logInit(start_time))
  def logSuccess(start_time: Long): ZIO[LoggingSupport, Throwable, Unit] = ZIO.accessM(_.get.logSuccess(start_time))
  def logError(start_time: Long, ex: Throwable): ZIO[LoggingSupport, Throwable, Unit] = ZIO.accessM(_.get.logError(start_time,ex))

  object StepLogger {
    def live[IP,OP](etlStep: EtlStep[IP,OP]): ZLayer[LoggerResourceClient, Throwable, LoggingSupport] = ZLayer.fromService { deps: LoggerResourceClient.Service =>
      new LoggerService {
        def logInit(step_start_time: Long): Task[Unit] = {
          if (deps.loggerResource.db.isDefined)
            deps.loggerResource.db.get.updateStepLevelInformation(step_start_time, etlStep, "started", mode = "insert").as(())
          else ZIO.unit
        }
        def logSuccess(step_start_time: Long): Task[Unit] = {
          deps.loggerResource.slack.foreach(_.updateStepLevelInformation(step_start_time, etlStep, "pass"))
          if (deps.loggerResource.db.isDefined)
            deps.loggerResource.db.get.updateStepLevelInformation(step_start_time, etlStep, "pass").as(())
          else
            ZIO.unit
        }
        def logError(step_start_time: Long, ex: Throwable): Task[Unit] = {
          deps.loggerResource.slack.foreach(_.updateStepLevelInformation(step_start_time, etlStep, "failed", Some(ex.getMessage)))
          if (deps.loggerResource.db.isDefined) {
            logger.error("Step Error StackTrace:"+"\n"+ex.getStackTrace.mkString("\n"))
            deps.loggerResource.db.get.updateStepLevelInformation(step_start_time, etlStep, "failed", Some(ex.getMessage)) *> Task.fail(new RuntimeException(ex.getMessage))
          }
          else
            Task.fail(ex)
        }
      }
    }
  }

  object JobLogger {
    def live(res: LoggerResource): LoggerService = new LoggerService {
      override def logInit(start_time: Long): Task[Unit] = {
        if (res.db.isDefined) res.db.get.updateJobInformation(start_time,"started","insert").as(())
        else
          ZIO.unit
      }
      override def logSuccess(start_time: Long): Task[Unit] = {
        for {
          _  <- UIO.succeed(if (res.slack.isDefined) res.slack.get.updateJobInformation(start_time,"pass"))
          _  <- if (res.db.isDefined) res.db.get.updateJobInformation(start_time,"pass") else ZIO.unit
          _  <- UIO.succeed(logger.info(s"Job completed successfully in ${UF.getTimeDifferenceAsString(start_time, UF.getCurrentTimestamp)}"))
        } yield ()
      }
      override def logError(start_time: Long, ex: Throwable): Task[Unit] = {
        if (res.slack.isDefined)
          res.slack.get.updateJobInformation(start_time,"failed")
        logger.error(s"Job completed with failure in ${UF.getTimeDifferenceAsString(start_time, UF.getCurrentTimestamp)}")
        if (res.db.isDefined)
          res.db.get.updateJobInformation(start_time,"failed",error_message = Some(ex.getMessage)).as(()) *> Task.fail(ex)
        else
          Task.fail(ex)
      }
    }
  }
}
