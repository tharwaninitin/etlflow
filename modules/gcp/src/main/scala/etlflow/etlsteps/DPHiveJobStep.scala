package etlflow.etlsteps

import gcp4zio._
import zio.RIO

case class DPHiveJobStep(name: String, query: String, clusterName: String, project: String, region: String)
    extends EtlStep[DPJobEnv, Unit] {

  final def process: RIO[DPJobEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting Hive Dataproc Job: $name")
    DPJobApi.executeHiveJob(query, clusterName, project, region)
  }

  override def getStepProperties: Map[String, String] = Map(
    "query"       -> query,
    "clusterName" -> clusterName,
    "project"     -> project,
    "region"      -> region
  )
}
