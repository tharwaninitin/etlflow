package etlflow.gcp

import com.google.cloud.dataproc.v1._
import com.google.protobuf.Duration
import etlflow.model.Credential.GCP
import etlflow.model.Executor.DATAPROC
import zio.{Managed, Task, TaskLayer}
import scala.jdk.CollectionConverters._

case class DP(client: ClusterControllerClient) extends DPApi.Service[Task] {

  def createDataproc(config: DATAPROC, props: DataprocProperties): Task[Cluster] = Task {
    val end_point_config = EndpointConfig.newBuilder().setEnableHttpPortAccess(true)
    val software_config  = SoftwareConfig.newBuilder().setImageVersion(props.image_version)
    val disk_config_m =
      DiskConfig
        .newBuilder()
        .setBootDiskType(props.boot_disk_type)
        .setBootDiskSizeGb(props.master_boot_disk_size_gb)
    val disk_config_w =
      DiskConfig
        .newBuilder()
        .setBootDiskType(props.boot_disk_type)
        .setBootDiskSizeGb(props.worker_boot_disk_size_gb)

    val gce_cluster_builder = props.subnet_uri match {
      case Some(value) =>
        GceClusterConfig
          .newBuilder()
          .setInternalIpOnly(true)
          .setSubnetworkUri(value)
          .addAllTags(props.network_tags.asJava)
          .addServiceAccountScopes("https://www.googleapis.com/auth/cloud-platform")
      case None =>
        GceClusterConfig
          .newBuilder()
          .setInternalIpOnly(true)
          .addAllTags(props.network_tags.asJava)
          .addServiceAccountScopes("https://www.googleapis.com/auth/cloud-platform")
    }

    val gce_cluster_config = props.service_account match {
      case Some(value) => gce_cluster_builder.setServiceAccount(value)
      case _           => gce_cluster_builder
    }

    val master_config = InstanceGroupConfig.newBuilder
      .setMachineTypeUri(props.master_machine_type_uri)
      .setNumInstances(props.master_num_instance)
      .setDiskConfig(disk_config_m)
      .build
    val worker_config = InstanceGroupConfig.newBuilder
      .setMachineTypeUri(props.worker_machine_type_uri)
      .setNumInstances(props.worker_num_instance)
      .setDiskConfig(disk_config_w)
      .build
    val cluster_config_builder = ClusterConfig.newBuilder
      .setMasterConfig(master_config)
      .setWorkerConfig(worker_config)
      .setSoftwareConfig(software_config)
      .setConfigBucket(props.bucket_name)
      .setGceClusterConfig(gce_cluster_config)
      .setEndpointConfig(end_point_config)

    val cluster_config = props.idle_deletion_duration_sec match {
      case Some(value) =>
        cluster_config_builder
          .setLifecycleConfig(
            LifecycleConfig.newBuilder().setIdleDeleteTtl(Duration.newBuilder().setSeconds(value))
          )
          .build
      case _ => cluster_config_builder.build
    }

    val cluster = Cluster.newBuilder.setClusterName(config.cluster_name).setConfig(cluster_config).build

    val create_cluster_async_request = client.createClusterAsync(config.project, config.region, cluster)

    val response = create_cluster_async_request.get
    logger.info(s"Cluster created successfully: ${response.getClusterName}")
    response
  }

  def deleteDataproc(config: DATAPROC): Task[Unit] = Task {
    val delete_cluster_async_request = client.deleteClusterAsync(config.project, config.region, config.cluster_name)
    val response                     = delete_cluster_async_request.get
    logger.info(s"Cluster ${config.cluster_name} successfully deleted. API response is ${response.toString}")
  }
}

object DP {
  def live(endpoint: String, credentials: Option[GCP] = None): TaskLayer[DPEnv] =
    Managed.fromAutoCloseable(Task(DPClient(endpoint, credentials))).map(dp => DP(dp)).toLayer
}
