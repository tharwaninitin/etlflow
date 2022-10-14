package etlflow.task

import gcp4zio.dp._
import zio.RIO

case class DPDeleteTask(name: String, cluster: String, project: String, region: String) extends EtlTask[DPEnv, Unit] {

  override protected def process: RIO[DPEnv, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting Delete Cluster Task: $name")
    logger.info(s"Cluster Name: $cluster and Region: $region")
    DPApi.deleteDataproc(cluster, project, region)
  }

  override def getTaskProperties: Map[String, String] = Map(
    "cluster" -> cluster,
    "project" -> project,
    "region"  -> region
  )
}
