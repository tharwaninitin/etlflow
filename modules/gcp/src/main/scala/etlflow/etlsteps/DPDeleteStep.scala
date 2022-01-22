package etlflow.etlsteps

import etlflow.gcp.{DP, DPApi}
import etlflow.model.Executor.DATAPROC
import zio.Task

class DPDeleteStep(
    val name: String,
    val config: DATAPROC
) extends EtlStep[Unit] {

  final def process: Task[Unit] = {
    val env = DP.live(config)
    logger.info("#" * 100)
    logger.info(s"Starting Delete Cluster Step: $name")
    logger.info(s"Cluster Name: ${config.cluster_name} and Region: ${config.region}")
    DPApi.deleteDataproc().provideLayer(env)
  }

  override def getStepProperties: Map[String, String] =
    Map(
      "name"   -> name,
      "config" -> config.toString
    )
}

object DPDeleteStep {
  def apply(name: String, config: DATAPROC): DPDeleteStep = new DPDeleteStep(name, config)
}
