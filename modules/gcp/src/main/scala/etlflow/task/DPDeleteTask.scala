package etlflow.task

import gcp4zio.dp._
import zio.RIO

case class DPDeleteTask(name: String, cluster: String) extends EtlTask[DPCluster, Unit] {

  override protected def process: RIO[DPCluster, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Starting Delete Cluster Task: $name")
    DPCluster.deleteDataproc(cluster)
  }

  override val metadata: Map[String, String] = Map("cluster" -> cluster)
}
