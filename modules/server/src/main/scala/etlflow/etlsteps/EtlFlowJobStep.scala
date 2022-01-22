package etlflow.etlsteps

import etlflow.EtlJobProps
import etlflow.etljobs.EtlJob
import etlflow.utils.Configuration
import zio.{RIO, ZEnv}

class EtlFlowJobStep[EJP <: EtlJobProps] private (val name: String, job: => EtlJob[EJP]) extends EtlStep[ZEnv, Unit] {

  lazy val job_instance = job
  var job_run_id        = java.util.UUID.randomUUID.toString

  final def process: RIO[ZEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting EtlFlowJobStep for: $name")
    for {
      cfg <- Configuration.config
      _ <- job_instance
        .execute()
        .provideSomeLayer[ZEnv](etlflow.log.DB(cfg.db.get, job_run_id, "EtlFlowJobStep-" + name + "-Pool", 1))
    } yield ()
  }

  override def getStepProperties: Map[String, String] =
    Map("step_run_id" -> job_run_id)
}

object EtlFlowJobStep {
  def apply[EJP <: EtlJobProps](name: String, job: => EtlJob[EJP]): EtlFlowJobStep[EJP] =
    new EtlFlowJobStep[EJP](name, job)
}
