package etlflow.etlsteps

import etlflow.gcp.{DPJobApi, DPJobEnv}
import etlflow.model.Executor.DATAPROC
import zio.RIO

case class DPHiveJobStep(name: String, query: String, config: DATAPROC) extends EtlStep[DPJobEnv, Unit] {

  final def process: RIO[DPJobEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting Hive Dataproc Job: $name")
    DPJobApi.executeHiveJob(query, config)
  }

  override def getStepProperties: Map[String, String] = Map("query" -> query)
}
