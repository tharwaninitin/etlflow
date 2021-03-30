package etlflow.log

import etlflow.etlsteps.EtlStep
import etlflow.utils.{UtilityFunctions => UF}
import etlflow.{JobLogger, StepLogger}
import zio.{Has, Task, ZIO, ZLayer}

object EtlLogger extends ApplicationLogger {

  type LoggingSupport = Has[LoggerService]
  trait LoggerService {
    def logInit(start_time: Long): Task[Unit]
    def logSuccess(start_time: Long): Task[Unit]
    def logError(start_time: Long, ex: Throwable): Task[Unit]
  }
  def logInit(start_time: Long): ZIO[LoggingSupport, Throwable, Unit] = ZIO.accessM(_.get.logInit(start_time))
  def logSuccess(start_time: Long): ZIO[LoggingSupport, Throwable, Unit] = ZIO.accessM(_.get.logSuccess(start_time))
  def logError(start_time: Long, ex: Throwable): ZIO[LoggingSupport, Throwable, Unit] = ZIO.accessM(_.get.logError(start_time,ex))

  type StepLoggerResourceEnv = Has[StepLoggerResourceEnv.Service]
  object StepLoggerResourceEnv {
    trait Service {
      val res: StepLogger
    }
    val live: ZLayer[Has[StepLogger], Nothing, StepLoggerResourceEnv] = ZLayer.fromService { deps =>
      new StepLoggerResourceEnv.Service { val res: StepLogger = deps }
    }
  }

  object StepLoggerEnv {
    def live[IP,OP](etlStep: EtlStep[IP,OP]): ZLayer[StepLoggerResourceEnv, Throwable, LoggingSupport] = ZLayer.fromService { deps: StepLoggerResourceEnv.Service =>
      new LoggerService {
        def logInit(step_start_time: Long): Task[Unit] = {
          if(deps.res.db.isDefined)
            deps.res.db.get.updateStepLevelInformation(step_start_time, etlStep, "started", mode = "insert").as(())
          else
            ZIO.unit
        }
        def logSuccess(step_start_time: Long): Task[Unit] = {
          deps.res.slack.foreach(_.updateStepLevelInformation(step_start_time, etlStep, "pass"))
          if(deps.res.db.isDefined)
            deps.res.db.get.updateStepLevelInformation(step_start_time, etlStep, "pass").as(())
          else
            ZIO.unit
        }
        def logError(step_start_time: Long, ex: Throwable): Task[Unit] = {
          logger.error("Step Error StackTrace:"+"\n"+ex.getStackTrace.mkString("\n"))
          deps.res.slack.foreach(_.updateStepLevelInformation(step_start_time, etlStep, "failed", Some(ex.getMessage)))
          if(deps.res.db.isDefined)
            deps.res.db.get.updateStepLevelInformation(step_start_time, etlStep, "failed", Some(ex.getMessage)) *> Task.fail(new RuntimeException(ex.getMessage))
          else
            ZIO.unit
        }
      }
    }
  }

  object JobLoggerEnv {
    def live(res: JobLogger, job_type: String): LoggerService = new LoggerService {
      override def logInit(start_time: Long): Task[Unit] = {
        if (res.db.isDefined) res.db.get.logStart(start_time,job_type) else ZIO.unit
      }
      override def logSuccess(start_time: Long): Task[Unit] = {
        logger.info(s"Job completed successfully in ${UF.getTimeDifferenceAsString(start_time, UF.getCurrentTimestamp)}")
        res.slack.foreach(_.updateJobInformation(start_time,"pass",job_type = job_type))
        if (res.db.isDefined) res.db.get.logEnd(start_time) else ZIO.unit
      }
      override def logError(start_time: Long, ex: Throwable): Task[Unit] = {
        logger.error(s"Job completed with failure in ${UF.getTimeDifferenceAsString(start_time, UF.getCurrentTimestamp)}")
        res.slack.foreach(_.updateJobInformation(start_time,"failed",job_type = job_type))
        if (res.db.isDefined) res.db.get.logEnd(start_time, Some(ex.getMessage)).as(()) *> Task.fail(ex) else Task.fail(ex)
      }
    }
  }
}
