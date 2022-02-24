package etlflow.etlsteps

import gcp4zio._
import zio.RIO
import zio.blocking.Blocking

case class DPDeleteStep(name: String, clusterName: String, project: String, region: String)
    extends EtlStep[DPEnv with Blocking, Unit] {

  protected def process: RIO[DPEnv with Blocking, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting Delete Cluster Step: $name")
    logger.info(s"Cluster Name: $clusterName and Region: $region")
    DPApi.deleteDataproc(clusterName, project, region)
  }

  override def getStepProperties: Map[String, String] = Map(
    "clusterName" -> clusterName,
    "project"     -> project,
    "region"      -> region
  )
}
