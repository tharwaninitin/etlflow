package etlflow.etlsteps

import etlflow.gcp.{DPApi, DPEnv}
import etlflow.model.Executor.DATAPROC
import zio.RIO

case class DPDeleteStep(name: String, config: DATAPROC) extends EtlStep[DPEnv, Unit] {

  final def process: RIO[DPEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting Delete Cluster Step: $name")
    logger.info(s"Cluster Name: ${config.cluster_name} and Region: ${config.region}")
    DPApi.deleteDataproc(config)
  }

  override def getStepProperties: Map[String, String] = Map("name" -> name, "config" -> config.toString)
}
