package etlflow.etlsteps

import etlflow.EtlJobProps
import etlflow.core.CoreEnv
import etlflow.etljobs.EtlJob
import etlflow.schema.LoggingLevel
import etlflow.utils.Configuration
import zio.RIO

class EtlFlowJobStep[EJP <: EtlJobProps] private(val name: String, job: => EtlJob[EJP]) extends EtlStep[Unit,Unit] {

  lazy val job_instance = job
  var job_run_id = java.util.UUID.randomUUID.toString

  final def process(in: =>Unit): RIO[CoreEnv, Unit] = {
    logger.info("#"*100)
    logger.info(s"Starting EtlFlowJobStep for: $name")
    val layer = etlflow.log.Implementation.live ++ etlflow.log.SlackImplementation.nolog ++ etlflow.log.ConsoleImplementation.live
    for {
      cfg <- Configuration.config
      _   <- job_instance.execute(Some(job_run_id), Some("false")).provideSomeLayer[CoreEnv](layer ++ etlflow.log.DBLiveImplementation(cfg.db.get, "EtlFlowJobStep-" + name + "-Pool", 1))
    } yield ()
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] =  {
    Map("step_run_id" -> job_run_id)
  }
}

object EtlFlowJobStep {
  def apply[EJP <: EtlJobProps](name: String, job: => EtlJob[EJP]): EtlFlowJobStep[EJP] =
    new EtlFlowJobStep[EJP](name, job)
}