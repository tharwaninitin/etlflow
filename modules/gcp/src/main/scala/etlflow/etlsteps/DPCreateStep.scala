package etlflow.etlsteps

import com.google.cloud.dataproc.v1.Cluster
import etlflow.gcp.{DP, DPService, DataprocProperties}
import etlflow.schema.Executor.DATAPROC
import etlflow.schema.LoggingLevel
import zio.Task

class DPCreateStep(
                     val name: String,
                     val config: DATAPROC,
                     val props: DataprocProperties
                   ) extends EtlStep[Unit,Cluster] {
  final def process(in: =>Unit): Task[Cluster] = {
    val env = DP.live(config)
    logger.info("#"*100)
    logger.info(s"Starting Create Cluster Step: $name")
    logger.info(s"Cluster Name: ${config.cluster_name} and Region: ${config.region}")
    DPService.createDataproc(props).provideLayer(env)
  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map(
      "name" -> name,
      "config" -> config.toString,
      "properties" -> props.toString
    )
}
object DPCreateStep {
  def apply(name: String, config: DATAPROC, props: DataprocProperties) : DPCreateStep = new DPCreateStep(name, config,props)
}
