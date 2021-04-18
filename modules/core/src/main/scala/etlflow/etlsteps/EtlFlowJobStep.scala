package etlflow.etlsteps

import etlflow.EtlJobProps
import etlflow.etljobs.EtlJob
import etlflow.executor.LocalExecutor
import etlflow.jdbc.DbManager
import etlflow.utils.{Configuration, LoggingLevel}
import zio.{Task, ZEnv}

class EtlFlowJobStep[EJP <: EtlJobProps] private(val name: String, job: => EtlJob[EJP])
  extends EtlStep[Unit,Unit] with Configuration with DbManager {

  lazy val job_instance = job
  val job_run_id = java.util.UUID.randomUUID.toString

  final def process(in: =>Unit): Task[Unit] = {
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting EtlFlowJobStep for: $name")
    job_instance.execute(Some(job_run_id),Some("false"))
      .provideCustomLayer(liveTransactor(config.dbLog, "Step-" + name +"-Pool", 2))
      .provideLayer(ZEnv.live)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] =  {
    Map("step_run_id" -> job_run_id)
  }
}

object EtlFlowJobStep {
  def apply[EJP <: EtlJobProps](name: String, job: => EtlJob[EJP]): EtlFlowJobStep[EJP] =
    new EtlFlowJobStep[EJP](name, job)
}