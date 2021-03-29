package etlflow.log

import cats.effect.Blocker
import etlflow.{DbJobStepLogger, EtlJobProps}
import etlflow.log.DbJobLogger.createDbTransactorManaged
import etlflow.utils.Config
import zio.Managed
import scala.concurrent.ExecutionContext

object DbLogManager {
  def create(config: Config, ec: ExecutionContext, blocker: Blocker, pool_name: String = "LoggerPool", job_name: String, job_properties: EtlJobProps, job_run_id:String, is_master:String): Managed[Throwable, DbJobStepLogger] =
    if (job_properties.job_enable_db_logging)
      createDbTransactorManaged(config.dbLog,ec,pool_name)(blocker).map { transactor =>
          DbJobStepLogger(
            Some(new DbJobLogger(transactor, job_name, job_properties, job_run_id, is_master)),
            Some(new DbStepLogger(transactor, job_properties, job_run_id))
          )
      }
    else
      Managed.unit.map(_ => DbJobStepLogger(None,None))
}
