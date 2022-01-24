package etlflow.etlsteps

import com.google.cloud.dataproc.v1.Cluster
import etlflow.gcp.{DPApi, DPEnv, DataprocProperties}
import etlflow.model.Executor.DATAPROC
import zio.RIO

case class DPCreateStep(name: String, config: DATAPROC, props: DataprocProperties) extends EtlStep[DPEnv, Cluster] {
  final def process: RIO[DPEnv, Cluster] = {
    logger.info("#" * 100)
    logger.info(s"Starting Create Cluster Step: $name")
    logger.info(s"Cluster Name: ${config.cluster_name} and Region: ${config.region}")
    DPApi.createDataproc(config, props)
  }

  override def getStepProperties: Map[String, String] =
    Map(
      "name"       -> name,
      "config"     -> config.toString,
      "properties" -> props.toString
    )
}
