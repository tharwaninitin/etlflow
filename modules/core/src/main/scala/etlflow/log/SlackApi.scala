package etlflow.log

import etlflow.etlsteps.EtlStep
import etlflow.schema.LoggingLevel
import etlflow.utils.ApplicationLogger
import zio.{RIO, Task, UIO, URIO, ZIO}

object SlackApi extends ApplicationLogger {
  trait Service {
    def getSlackNotification: UIO[String]
    def logStepEnd(start_time: Long, job_notification_level: LoggingLevel, etlstep: EtlStep[_, _], error_message: Option[String] = None): Task[Unit]
    def logJobEnd(job_name: String, job_run_id: String, job_notification_level: LoggingLevel, start_time: Long, error_message: Option[String] = None): Task[Unit]
  }

  def logStepEnd(start_time: Long, job_notification_level: LoggingLevel, etlstep: EtlStep[_, _], error_message: Option[String] = None): RIO[SlackLogEnv, Unit] =
    ZIO.accessM[SlackLogEnv](_.get.logStepEnd(start_time, job_notification_level, etlstep, error_message))
  def logJobEnd(job_name: String, job_run_id: String, job_notification_level: LoggingLevel, start_time: Long, error_message: Option[String] = None): RIO[SlackLogEnv, Unit] =
    ZIO.accessM[SlackLogEnv](_.get.logJobEnd(job_name, job_run_id, job_notification_level, start_time, error_message))
  def getSlackNotification: URIO[SlackLogEnv, String] = ZIO.accessM[SlackLogEnv](_.get.getSlackNotification)
}
