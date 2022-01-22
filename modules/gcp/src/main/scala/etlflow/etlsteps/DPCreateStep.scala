package etlflow.etlsteps

import com.google.cloud.dataproc.v1.Cluster
import etlflow.gcp.{DP, DPApi, DataprocProperties}
import etlflow.model.Executor.DATAPROC
import zio.Task

class DPCreateStep(
    val name: String,
    val config: DATAPROC,
    val props: DataprocProperties
) extends EtlStep[Cluster] {
  final def process: Task[Cluster] = {
    val env = DP.live(config)
    logger.info("#" * 100)
    logger.info(s"Starting Create Cluster Step: $name")
    logger.info(s"Cluster Name: ${config.cluster_name} and Region: ${config.region}")
    DPApi.createDataproc(props).provideLayer(env)
  }

  override def getStepProperties: Map[String, String] =
    Map(
      "name"       -> name,
      "config"     -> config.toString,
      "properties" -> props.toString
    )
}
object DPCreateStep {
  def apply(name: String, config: DATAPROC, props: DataprocProperties): DPCreateStep =
    new DPCreateStep(name, config, props)
}
