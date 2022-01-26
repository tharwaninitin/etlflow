package etlflow.gcp

import com.google.cloud.dataproc.v1._
import com.google.protobuf.Duration
import etlflow.model.Executor.DATAPROC
import zio.{Task, UIO, ULayer}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

case class DP() extends DPApi.Service[Task] {

  private def submitAndWait(jobCC: JobControllerClient, projectId: String, region: String, job: Job): Unit = {
    val request = jobCC.submitJob(projectId, region, job)
    val jobId   = request.getReference.getJobId
    logger.info(s"Submitted job $jobId")
    var continue = true
    var jobInfo  = jobCC.getJob(projectId, region, jobId)
    var jobState = jobInfo.getStatus.getState.toString
    while (continue) {
      jobInfo = jobCC.getJob(projectId, region, jobId)
      jobState = jobInfo.getStatus.getState.toString
      logger.info(s"Job $jobId Status $jobState")
      jobInfo.getStatus.getState.toString match {
        case "DONE" =>
          logger.info(s"Job $jobId completed successfully with state $jobState")
          jobCC.close()
          continue = false
        case "CANCELLED" | "ERROR" =>
          val error = jobInfo.getStatus.getDetails
          logger.error(s"Job $jobId failed with error $error")
          jobCC.close()
          throw new RuntimeException(s"Job failed with error $error")
        case _ =>
          TimeUnit.SECONDS.sleep(10)
      }
    }
  }

  def createDataproc(config: DATAPROC, props: DataprocProperties): Task[Cluster] = Task {
    val end_point_config            = EndpointConfig.newBuilder().setEnableHttpPortAccess(true)
    val cluster_controller_settings = ClusterControllerSettings.newBuilder.setEndpoint(config.endpoint).build
    val cluster_controller_client   = ClusterControllerClient.create(cluster_controller_settings)
    val software_config             = SoftwareConfig.newBuilder().setImageVersion(props.image_version)
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

    try {
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
      val create_cluster_async_request =
        cluster_controller_client.createClusterAsync(config.project, config.region, cluster)
      val response = create_cluster_async_request.get
      logger.info(s"Cluster created successfully: ${response.getClusterName}")
      response
    } catch {
      case e: Throwable =>
        logger.error(s"Error creating cluster: ${e.getMessage} ")
        throw e
    } finally if (cluster_controller_client != null) cluster_controller_client.close()
  }

  def deleteDataproc(config: DATAPROC): Task[Unit] = Task {
    val cluster_controller_settings = ClusterControllerSettings.newBuilder.setEndpoint(config.endpoint).build
    val cluster_controller_client   = ClusterControllerClient.create(cluster_controller_settings)

    try {
      val delete_cluster_async_request =
        cluster_controller_client.deleteClusterAsync(config.project, config.region, config.cluster_name)
      val response = delete_cluster_async_request.get
      logger.info(s"Cluster ${config.cluster_name} successfully deleted. API response is ${response.toString}")
    } catch {
      case e: Throwable =>
        logger.error(s"Error executing deleteCluster: ${e.getMessage} ")
        throw e
    } finally if (cluster_controller_client != null) cluster_controller_client.close()
  }

  def executeSparkJob(args: List[String], main_class: String, libs: List[String], config: DATAPROC): Task[Unit] = Task {
    logger.info(s"""Trying to submit spark job on Dataproc with Configurations:
                   |dp_region => ${config.region}
                   |dp_project => ${config.project}
                   |dp_endpoint => ${config.endpoint}
                   |dp_cluster_name => ${config.cluster_name}
                   |main_class => $main_class
                   |args => $args
                   |spark_conf => ${config.sp}""".stripMargin)
    logger.info("dp_libs")
    libs.foreach(logger.info)
    val jobControllerSettings = JobControllerSettings.newBuilder().setEndpoint(config.endpoint).build()
    val jobControllerClient   = JobControllerClient.create(jobControllerSettings)
    val jobPlacement          = JobPlacement.newBuilder().setClusterName(config.cluster_name).build()
    val spark_conf            = config.sp.map(x => (x.key, x.value)).toMap
    val sparkJob = SparkJob
      .newBuilder()
      .addAllJarFileUris(libs.asJava)
      .putAllProperties(spark_conf.asJava)
      .setMainClass(main_class)
      .addAllArgs(args.asJava)
      .build()
    val job: Job = Job.newBuilder().setPlacement(jobPlacement).setSparkJob(sparkJob).build()
    submitAndWait(jobControllerClient, config.project, config.region, job)
  }

  def executeHiveJob(query: String, config: DATAPROC): Task[Unit] = Task {
    logger.info(s"""Trying to submit hive job on Dataproc with Configurations:
                   |dp_region => ${config.region}
                   |dp_project => ${config.project}
                   |dp_endpoint => ${config.endpoint}
                   |dp_cluster_name => ${config.cluster_name}
                   |query => $query""".stripMargin)
    val jobControllerSettings = JobControllerSettings.newBuilder().setEndpoint(config.endpoint).build()
    val jobControllerClient   = JobControllerClient.create(jobControllerSettings)
    val jobPlacement          = JobPlacement.newBuilder().setClusterName(config.cluster_name).build()
    val queryList             = QueryList.newBuilder().addQueries(query)
    val hiveJob               = HiveJob.newBuilder().setQueryList(queryList).build()
    val job                   = Job.newBuilder().setPlacement(jobPlacement).setHiveJob(hiveJob).build()
    submitAndWait(jobControllerClient, config.project, config.region, job)
  }
}

object DP {
  lazy val live: ULayer[DPEnv] = UIO(DP()).toLayer
}
