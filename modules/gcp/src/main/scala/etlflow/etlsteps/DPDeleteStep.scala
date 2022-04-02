package etlflow.etlsteps

import gcp4zio._
import zio.RIO
import zio.blocking.Blocking

case class DPDeleteStep(name: String, cluster: String, project: String, region: String) extends EtlStep[Unit] {
  override protected type R = DPEnv with Blocking

  override protected def process: RIO[DPEnv with Blocking, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting Delete Cluster Step: $name")
    logger.info(s"Cluster Name: $cluster and Region: $region")
    DPApi.deleteDataproc(cluster, project, region)
  }

  override def getStepProperties: Map[String, String] = Map(
    "cluster" -> cluster,
    "project" -> project,
    "region"  -> region
  )
}
