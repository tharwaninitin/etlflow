package etlflow.log

import cats.effect.Blocker
import etlflow.EtlJobProps
import etlflow.log.DbJobLogger.createDbTransactorManaged
import etlflow.utils.{Config, LoggingLevel}
import zio.Managed

import scala.concurrent.ExecutionContext

class DbLogger(val job: Option[DbJobLogger], val step: Option[DbStepLogger])

object DbLogger {
  def apply(job_name: String, job_properties: EtlJobProps, config: Config, ec: ExecutionContext, blocker: Blocker, pool_name: String = "LoggerPool", job_run_id:String, is_master:String,job_notification_level:LoggingLevel,job_enable_db_logging:Boolean): Managed[Throwable, DbLogger] =
    if (job_enable_db_logging)
      createDbTransactorManaged(config.dbLog,ec,pool_name)(blocker).map { transactor =>
        new DbLogger(
            Some(new DbJobLogger(transactor, job_name, job_properties, job_run_id, is_master)),
            Some(new DbStepLogger(transactor, job_run_id,job_notification_level))
          )
      }
    else
      Managed.unit.map(_ => new DbLogger(None,None))
}
