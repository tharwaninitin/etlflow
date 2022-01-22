package etlflow.etlsteps

import etlflow.gcp.{DP, DPApi}
import etlflow.model.Executor.DATAPROC
import zio.Task

case class DPHiveJobStep(
    name: String,
    query: String,
    config: DATAPROC
) extends EtlStep[Unit] {

  final def process: Task[Unit] = {
    val env = DP.live(config)
    logger.info("#" * 100)
    logger.info(s"Starting Hive Dataproc Job: $name")
    DPApi.executeHiveJob(query).provideLayer(env)
  }

  override def getStepProperties: Map[String, String] = Map("query" -> query)
}
