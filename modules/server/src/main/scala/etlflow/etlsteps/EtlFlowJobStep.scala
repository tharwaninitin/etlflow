package etlflow.etlsteps

import etlflow.EtlJobProps
import etlflow.core.CoreEnv
import etlflow.etljobs.EtlJob
import etlflow.utils.Configuration
import zio.RIO

class EtlFlowJobStep[EJP <: EtlJobProps] private(val name: String, job: => EtlJob[EJP]) extends EtlStep[Unit,Unit] {

  lazy val job_instance = job
  var job_run_id = java.util.UUID.randomUUID.toString

  final def process(in: =>Unit): RIO[CoreEnv, Unit] = {
    logger.info("#"*100)
    logger.info(s"Starting EtlFlowJobStep for: $name")
    for {
      cfg <- Configuration.config
      _   <- job_instance.execute().provideSomeLayer[CoreEnv](etlflow.log.DB(cfg.db.get, job_run_id, "EtlFlowJobStep-" + name + "-Pool", 1))
    } yield ()
  }

  override def getStepProperties: Map[String, String] =  {
    Map("step_run_id" -> job_run_id)
  }
}

object EtlFlowJobStep {
  def apply[EJP <: EtlJobProps](name: String, job: => EtlJob[EJP]): EtlFlowJobStep[EJP] =
    new EtlFlowJobStep[EJP](name, job)
}