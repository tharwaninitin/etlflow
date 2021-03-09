package etlflow.etlsteps

import etlflow.gcp.{DP, DPService}
import etlflow.utils.Executor.DATAPROC
import etlflow.utils.LoggingLevel
import zio.Task

class DPDeleteStep (
                       val name: String,
                       val config: DATAPROC,
                     ) extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): Task[Unit] = {
    val env = DP.live(config)
    etl_logger.info("#" * 100)
    etl_logger.info(s"Starting Delete Cluster Step: $name")
    etl_logger.info(s"Cluster Name: ${config.cluster_name} and Region: ${config.region}")
    DPService.deleteDataproc().provideLayer(env)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map(
      "name" -> name,
      "config" -> config.toString
    )
}

object DPDeleteStep {
  def apply(name: String, config: DATAPROC) : DPDeleteStep = new DPDeleteStep(name, config)
}

