package etlflow.etlsteps

import com.google.cloud.dataproc.v1.Cluster
import gcp4zio._
import zio.RIO
import zio.blocking.Blocking

case class DPCreateStep(name: String, clusterName: String, project: String, region: String, props: DataprocProperties)
    extends EtlStep[DPEnv with Blocking, Cluster] {

  protected def process: RIO[DPEnv with Blocking, Cluster] = {
    logger.info("#" * 100)
    logger.info(s"Starting Create Cluster Step: $name")
    logger.info(s"Cluster Name: $clusterName and Region: $region")
    DPApi.createDataproc(clusterName, project, region, props)
  }

  override def getStepProperties: Map[String, String] = Map(
    "cluster"    -> clusterName,
    "project"    -> project,
    "region"     -> region,
    "properties" -> props.toString
  )
}
