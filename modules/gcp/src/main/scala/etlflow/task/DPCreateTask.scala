package etlflow.task

import com.google.cloud.dataproc.v1.Cluster
import gcp4zio.dp._
import zio.RIO

case class DPCreateTask(name: String, cluster: String, project: String, region: String, props: ClusterProps)
    extends EtlTask[DPEnv, Cluster] {

  override protected def process: RIO[DPEnv, Cluster] = {
    logger.info("#" * 100)
    logger.info(s"Starting Create Cluster Task: $name")
    logger.info(s"Cluster: $cluster and Region: $region")
    DPApi.createDataproc(cluster, project, region, props)
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getTaskProperties: Map[String, String] = Map(
    "cluster"    -> cluster,
    "project"    -> project,
    "region"     -> region,
    "properties" -> props.toString
  )
}
