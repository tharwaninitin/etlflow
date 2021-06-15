package etlflow.log

import etlflow.etlsteps.EtlStep
import etlflow.jdbc.{DB, DBEnv}
import etlflow.utils.{JsonJackson, LoggingLevel, UtilityFunctions => UF}
import zio.{Has, Task, ZIO, ZLayer}

object StepLogger extends ApplicationLogger {

  class StepHelper(etlStep: EtlStep[_,_], job_run_id: String, job_notification_level: LoggingLevel = LoggingLevel.INFO) extends ApplicationLogger {
    val remoteStep = List("EtlFlowJobStep", "DPSparkJobStep", "ParallelETLStep")

    def updateStepLevelInformation(start_time: Long, state_status: String, error_message: Option[String] = None, mode: String = "update"): ZIO[DBEnv, Throwable, Unit] =
    {
      val step_name = UF.stringFormatter(etlStep.name)
      val properties = JsonJackson.convertToJson(etlStep.getStepProperties(job_notification_level))

      if (mode == "insert") {
        val step_run_id = if (remoteStep.contains(etlStep.step_type)) etlStep.getStepProperties(job_notification_level)("step_run_id") else ""
        logger.info(s"Inserting step info for $step_name in db with status => ${state_status.toLowerCase()}")
        DB.insertStepRun(job_run_id, step_name, properties, etlStep.step_type, step_run_id, start_time)
      }
      else {
        val status = if (error_message.isDefined) state_status.toLowerCase() + " with error: " + error_message.get else state_status.toLowerCase()
        val elapsed_time = UF.getTimeDifferenceAsString(start_time, UF.getCurrentTimestamp)
        logger.info(s"Updating step info for $step_name in db with status => $status")
        DB.updateStepRun(job_run_id, step_name, properties, status, elapsed_time)
      }
    }
  }

  case class StepReq(job_run_id: String, slack: Option[SlackLogger] = None, job_notification_level: LoggingLevel = LoggingLevel.INFO)

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
        val stepLogger = new StepHelper(etlStep, deps.res.job_run_id)
        def logInit(start_time: Long): ZIO[DBEnv, Throwable, Unit] = {
          stepLogger.updateStepLevelInformation(start_time, "started", mode = "insert")
        }
        def logSuccess(start_time: Long): ZIO[DBEnv, Throwable, Unit] = {
          deps.res.slack.foreach(_.logStepEnd(start_time, etlStep))
          stepLogger.updateStepLevelInformation(start_time, "pass")
        }
        def logError(start_time: Long, ex: Throwable): ZIO[DBEnv, Throwable, Unit] = {
          logger.error("Step Error StackTrace:"+"\n"+ex.getStackTrace.mkString("\n"))
          deps.res.slack.foreach(_.logStepEnd(start_time, etlStep, Some(ex.getMessage)))
          stepLogger.updateStepLevelInformation(start_time, "failed", Some(ex.getMessage)) *> Task.fail(new RuntimeException(ex.getMessage))
        }
      }
    }
  }
}
