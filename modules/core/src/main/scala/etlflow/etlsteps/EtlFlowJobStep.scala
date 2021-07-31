package etlflow.etlsteps

import etlflow.etljobs.EtlJob
import etlflow.schema.LoggingLevel
import etlflow.utils.Configuration
import etlflow.{EtlJobProps, JobEnv}
import zio.RIO

class EtlFlowJobStep[EJP <: EtlJobProps] private(val name: String, job: => EtlJob[EJP]) extends EtlStep[Unit,Unit] {

  lazy val job_instance = job
  var job_run_id = java.util.UUID.randomUUID.toString

  final def process(in: =>Unit): RIO[JobEnv, Unit] = {
    logger.info("#"*100)
    logger.info(s"Starting EtlFlowJobStep for: $name")
    Configuration.config.flatMap(cfg => job_instance.execute(Some(cfg),Some(job_run_id),Some("false"), "{}"))
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] =  {
    Map("step_run_id" -> job_run_id)
  }
}

object EtlFlowJobStep {
  def apply[EJP <: EtlJobProps](name: String, job: => EtlJob[EJP]): EtlFlowJobStep[EJP] =
    new EtlFlowJobStep[EJP](name, job)
}