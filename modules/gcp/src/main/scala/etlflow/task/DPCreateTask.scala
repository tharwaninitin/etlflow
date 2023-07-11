package etlflow.task

import com.google.cloud.dataproc.v1.Cluster
import gcp4zio.dp._
import zio.RIO

case class DPCreateTask(name: String, cluster: String, props: ClusterProps) extends EtlTask[DPCluster, Cluster] {

  override protected def process: RIO[DPCluster, Cluster] = {
    logger.info("#" * 100)
    logger.info(s"Starting Cluster creation: $cluster")
    DPCluster.createDataproc(cluster, props)
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getMetaData: Map[String, String] = Map(
    "cluster"    -> cluster,
    "properties" -> props.toString
  )
}
