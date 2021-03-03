package etlflow.etlsteps

import etlflow.utils.LoggingLevel
import zio.Task
import com.google.cloud.dataproc.v1.{Cluster, ClusterConfig, ClusterControllerClient, ClusterControllerSettings, DiskConfig, EndpointConfig, GceClusterConfig, InstanceGroupConfig, LifecycleConfig, SoftwareConfig}

import com.google.protobuf.Duration
import etlflow.etlsteps.DPCreateStep.{DPConfig}

import scala.collection.JavaConverters._


class DPCreateStep(
                    val name: String,
                    val cluster_name: String,
                    val props: DPConfig
                  ) extends EtlStep[Unit,Unit] {
  final def process(in: =>Unit): Task[Unit] = Task{
    etl_logger.info("#"*100)
    etl_logger.info(s"Starting Create Cluster Step: $name")
    etl_logger.info(s"Cluster Name: $cluster_name and Region:" + props.region)

    val project_id =  props.project_id
    val region = props.region
    val endpoint = props.endpoint
    val bucket_name = props.bucket_name

    val end_point_config = EndpointConfig.newBuilder().setEnableHttpPortAccess(true)
    val cluster_controller_settings = ClusterControllerSettings.newBuilder.setEndpoint(endpoint).build
    val cluster_controller_client = ClusterControllerClient.create(cluster_controller_settings)
    val software_config = SoftwareConfig.newBuilder().setImageVersion(props.image_version)
    val disk_config_m = DiskConfig.newBuilder().setBootDiskType(props.boot_disk_type).setBootDiskSizeGb(props.master_boot_disk_size.toInt)
    val disk_config_w = DiskConfig.newBuilder().setBootDiskType(props.boot_disk_type).setBootDiskSizeGb(props.worker_boot_disk_size.toInt)

    val gce_cluster_builder = GceClusterConfig.newBuilder()
      .setInternalIpOnly(true)
      .setSubnetworkUri(props.subnet_work_uri)
      .addAllTags("non-prod-hbase-dataproc-egress-fw,non-prod-hbase-dataproc-ingress-fw,non-prod-hbase-dataproc-internal-ingress-fw".split(",").toList.asJava)
      .addServiceAccountScopes("https://www.googleapis.com/auth/cloud-platform")

    val gce_cluster_config = props.service_account match {
      case Some(_) => gce_cluster_builder.setServiceAccount(props.service_account.get)
      case _ => gce_cluster_builder
    }
    try {
      val master_config = InstanceGroupConfig.newBuilder.setMachineTypeUri(props.master_machine_type_uri).setNumInstances(props.master_num_instance.toInt).setDiskConfig(disk_config_m).build
      val worker_config = InstanceGroupConfig.newBuilder.setMachineTypeUri(props.worker_machine_type_uri).setNumInstances(props.worker_num_instance.toInt).setDiskConfig(disk_config_w).build
      val cluster_config_builder = ClusterConfig.newBuilder
        .setMasterConfig(master_config)
        .setWorkerConfig(worker_config)
        .setSoftwareConfig(software_config)
        .setConfigBucket(bucket_name)
        .setGceClusterConfig(gce_cluster_config)
        .setEndpointConfig(end_point_config)

      val cluster_config = props.idle_deletion_duration_sec match {
        case Some(_) => cluster_config_builder
          .setLifecycleConfig(
            LifecycleConfig.newBuilder().setIdleDeleteTtl(Duration.newBuilder().setSeconds(props.idle_deletion_duration_sec.get.toLong))
          ).build
        case _       => cluster_config_builder.build
      }

      val cluster = Cluster.newBuilder.setClusterName(cluster_name).setConfig(cluster_config).build
      val create_cluster_async_request = cluster_controller_client.createClusterAsync(project_id, region, cluster)
      val response = create_cluster_async_request.get
      etl_logger.info(s"Cluster created successfully: ${response.getClusterName}")
    } catch {
      case e: Throwable =>
        etl_logger.error(s"Error executing createCluster: ${e.getMessage} ")
        throw e
    } finally if (cluster_controller_client != null) cluster_controller_client.close()
  }
  override def getStepProperties(level: LoggingLevel): Map[String, String] =
    Map(
      "name" -> name,
      "cluster_name" -> cluster_name,
      "service_account" -> props.service_account.getOrElse("NA"),
      "Configuration" -> props.toString()
    )

}
object DPCreateStep {

  case class DPConfig
  (
    project_id: String,
    region:String,
    endpoint:String,
    bucket_name:String,
    subnet_work_uri:String,
    all_tags:List[String] = List.empty,
    service_account: Option[String]= None,
    idle_deletion_duration_sec: Option[Int]= Some(1800),
    master_machine_type_uri:String="n1-standard-4",
    worker_machine_type_uri:String="n1-standard-4",
    image_version:String="1.5.4-debian10",
    boot_disk_type:String="pd-ssd",
    master_boot_disk_size:String="400",
    worker_boot_disk_size:String="200",
    master_num_instance:String="1",
    worker_num_instance:String="3"
  )

  def apply(
             name: String,
             cluster_name: String,
             props: DPConfig
           ) : DPCreateStep = new DPCreateStep(name, cluster_name,props)
}
