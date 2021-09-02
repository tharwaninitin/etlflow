package etlflow.etlsteps

import etlflow.crypto.CryptoEnv
import etlflow.db.DBEnv
import etlflow.etljobs.EtlJob
import etlflow.json.JsonEnv
import etlflow.schema.LoggingLevel
import etlflow.{CoreEnv, EtlJobProps}
import zio.RIO
import zio.blocking.Blocking
import zio.clock.Clock

class EtlFlowJobStep[EJP <: EtlJobProps] private(val name: String, job: => EtlJob[EJP]) extends EtlStep[Unit,Unit] {

  lazy val job_instance = job
  var job_run_id = java.util.UUID.randomUUID.toString

  final def process(in: =>Unit): RIO[CoreEnv, Unit] = {
    logger.info("#"*100)
    logger.info(s"Starting EtlFlowJobStep for: $name")
    job_instance.execute(Some(job_run_id), Some("false"))
      .provideSomeLayer[DBEnv with JsonEnv with CryptoEnv with Blocking with Clock](etlflow.log.Implementation.live ++ etlflow.log.SlackApi.nolog)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] =  {
    Map("step_run_id" -> job_run_id)
  }
}

object EtlFlowJobStep {
  def apply[EJP <: EtlJobProps](name: String, job: => EtlJob[EJP]): EtlFlowJobStep[EJP] =
    new EtlFlowJobStep[EJP](name, job)
}