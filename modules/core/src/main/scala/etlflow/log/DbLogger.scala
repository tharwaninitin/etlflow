package etlflow.log

import etlflow.{EtlJobProps, DBEnv}
import etlflow.utils.{Config, LoggingLevel}
import zio.{URIO, ZIO}

class DbLogger(val job: Option[DbJobLogger], val step: Option[DbStepLogger])

object DbLogger {
  def apply(job_name: String, job_properties: EtlJobProps, config: Config, job_run_id: String, is_master: String, job_notification_level: LoggingLevel, job_enable_db_logging: Boolean)
  : URIO[DBEnv, DbLogger] = ZIO.access[DBEnv] { x =>
    if (job_enable_db_logging)
      new DbLogger(
        Some(new DbJobLogger(x.get, job_name, job_properties, job_run_id, is_master)),
        Some(new DbStepLogger(x.get, job_run_id, job_notification_level))
      )
    else
      new DbLogger(None, None)
  }
}
