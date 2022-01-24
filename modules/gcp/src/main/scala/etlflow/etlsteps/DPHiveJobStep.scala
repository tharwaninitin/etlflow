package etlflow.etlsteps

import etlflow.gcp.{DPApi, DPEnv}
import etlflow.model.Executor.DATAPROC
import zio.RIO

case class DPHiveJobStep(name: String, query: String, config: DATAPROC) extends EtlStep[DPEnv, Unit] {

  final def process: RIO[DPEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting Hive Dataproc Job: $name")
    DPApi.executeHiveJob(query, config)
  }

  override def getStepProperties: Map[String, String] = Map("query" -> query)
}
