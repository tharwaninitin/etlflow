package etlflow.etlsteps

import java.util.concurrent.ExecutionException

import com.google.cloud.dataproc.v1._
import etlflow.utils.LoggingLevel
import zio.Task

class DPDeleteStep (
                       val name: String,
                       val cluster_name: String,
                       val props: Map[String,String],
                     ) extends EtlStep[Unit,Unit] {

  final def process(in: =>Unit): Task[Unit] = Task{
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting Delete Cluster Step: $name")
    etl_logger.info(s"Cluster Name: $cluster_name and Region:" + props("region"))

    val project_id = props("project_id")
    val endpoint = props("endpoint")
    val region = props("region")

    val cluster_controller_settings = ClusterControllerSettings.newBuilder.setEndpoint(endpoint).build
    val cluster_controller_client = ClusterControllerClient.create(cluster_controller_settings)
    try {
      val delete_cluster_async_request = cluster_controller_client.deleteClusterAsync(project_id, region, cluster_name)
      val response = delete_cluster_async_request.get
      etl_logger.info(s"Cluster $cluster_name successfully deleted. API response is ${response.toString}")
    } catch {
      case e: Throwable =>
        etl_logger.error(s"Error executing deleteCluster: ${e.getMessage} ")
        throw e
    } finally if (cluster_controller_client != null) cluster_controller_client.close()

  }

  override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map(
      "name" -> name,
      "cluster_name" -> cluster_name
    ) ++ props
}

object DPDeleteStep {
  def apply(
             name: String,
             cluster_name: String,
             props: Map[String,String]
           ) : DPDeleteStep = new DPDeleteStep(name, cluster_name, props)
}

