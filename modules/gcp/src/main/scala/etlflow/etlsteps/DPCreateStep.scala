package etlflow.etlsteps

import com.google.cloud.dataproc.v1.Cluster
import gcp4zio._
import zio.RIO
import zio.blocking.Blocking

case class DPCreateStep(name: String, cluster: String, project: String, region: String, props: DataprocProperties)
    extends EtlStep[Cluster] {
  override protected type R = DPEnv with Blocking

  override protected def process: RIO[DPEnv with Blocking, Cluster] = {
    logger.info("#" * 100)
    logger.info(s"Starting Create Cluster Step: $name")
    logger.info(s"Cluster: $cluster and Region: $region")
    DPApi.createDataproc(cluster, project, region, props)
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getStepProperties: Map[String, String] = Map(
    "cluster"    -> cluster,
    "project"    -> project,
    "region"     -> region,
    "properties" -> props.toString
  )
}
