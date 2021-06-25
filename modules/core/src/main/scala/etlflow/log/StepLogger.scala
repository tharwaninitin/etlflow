package etlflow.log

import etlflow.common.DateTimeFunctions._
import etlflow.db.{DBApi, DBEnv}
import etlflow.etlsteps.EtlStep
import etlflow.json.{Implementation, JsonService}
import etlflow.utils.{LoggingLevel, UtilityFunctions => UF}
import zio.{Has, Task, ZIO, ZLayer}

private[etlflow] object StepLogger extends ApplicationLogger {

  class StepLoggerHelper(etlStep: EtlStep[_,_], job_run_id: String, job_notification_level: LoggingLevel = LoggingLevel.INFO) extends ApplicationLogger {
    val remoteStep = List("EtlFlowJobStep", "DPSparkJobStep", "ParallelETLStep")
    def update(start_time: Long, state_status: String, error_message: Option[String] = None, mode: String = "update"): ZIO[DBEnv, Throwable, Unit] =
    {
      val step_name = UF.stringFormatter(etlStep.name)

      if (mode == "insert") {
        val step_run_id = if (remoteStep.contains(etlStep.step_type)) etlStep.getStepProperties(job_notification_level)("step_run_id") else ""
        for{
          properties <- JsonService.convertToJson(etlStep.getStepProperties(job_notification_level)).provideLayer(Implementation.live)
          _           = logger.info(s"Inserting step info for $step_name in db with status => ${state_status.toLowerCase()}")
          _ <- DBApi.insertStepRun(job_run_id, step_name, properties, etlStep.step_type, step_run_id, start_time)
        } yield ()
      }
      else {
        val status = if (error_message.isDefined) state_status.toLowerCase() + " with error: " + error_message.get else state_status.toLowerCase()
        val elapsed_time = getTimeDifferenceAsString(start_time, getCurrentTimestamp)
        for{
          properties <- JsonService.convertToJson(etlStep.getStepProperties(job_notification_level)).provideLayer(Implementation.live)
          _           = logger.info(s"Updating step info for $step_name in db with status => $status")
          _ <- DBApi.updateStepRun(job_run_id, step_name, properties, status, elapsed_time)
        } yield ()
      }
    }
  }

  type LoggingSupport = Has[LoggerService]
  trait LoggerService {
    def logInit(start_time: Long): ZIO[DBEnv, Throwable, Unit]
    def logSuccess(start_time: Long): ZIO[DBEnv, Throwable, Unit]
    def logError(start_time: Long, ex: Throwable): ZIO[DBEnv, Throwable, Unit]
  }
  def logInit(start_time: Long): ZIO[LoggingSupport with DBEnv, Throwable, Unit] = ZIO.accessM(_.get.logInit(start_time))
  def logSuccess(start_time: Long): ZIO[LoggingSupport with DBEnv, Throwable, Unit] = ZIO.accessM(_.get.logSuccess(start_time))
  def logError(start_time: Long, ex: Throwable): ZIO[LoggingSupport with DBEnv, Throwable, Unit] = ZIO.accessM(_.get.logError(start_time,ex))

  type StepLoggerResourceEnv = Has[StepLoggerResourceEnv.Service]
  object StepLoggerResourceEnv {
    trait Service {
      val res: StepReq
    }

    val live: ZLayer[Has[StepReq], Nothing, StepLoggerResourceEnv] = ZLayer.fromService { deps =>
      new StepLoggerResourceEnv.Service { val res: StepReq = deps }
    }
  }

  object StepLoggerImpl {
    def live[IP,OP](etlStep: EtlStep[IP,OP]): ZLayer[StepLoggerResourceEnv, Throwable, LoggingSupport] = ZLayer.fromService { deps: StepLoggerResourceEnv.Service =>
      new LoggerService {
        val stepLogger = new StepLoggerHelper(etlStep, deps.res.job_run_id)
        def logInit(start_time: Long): ZIO[DBEnv, Throwable, Unit] = {
          stepLogger.update(start_time, "started", mode = "insert")
        }
        def logSuccess(start_time: Long): ZIO[DBEnv, Throwable, Unit] = {
          deps.res.slack.foreach(_.logStepEnd(start_time, etlStep))
          stepLogger.update(start_time, "pass")
        }
        def logError(start_time: Long, ex: Throwable): ZIO[DBEnv, Throwable, Unit] = {
          logger.error("Step Error StackTrace:"+"\n"+ex.getStackTrace.mkString("\n"))
          deps.res.slack.foreach(_.logStepEnd(start_time, etlStep, Some(ex.getMessage)))
          stepLogger.update(start_time, "failed", Some(ex.getMessage)) *> Task.fail(new RuntimeException(ex.getMessage))
        }
      }
    }
  }
}
