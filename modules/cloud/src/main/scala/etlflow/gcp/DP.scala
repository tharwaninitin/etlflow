package etlflow.gcp

import java.util.concurrent.TimeUnit
import com.google.cloud.dataproc.v1.{Cluster, ClusterConfig, ClusterControllerClient, ClusterControllerSettings, DiskConfig, EndpointConfig, GceClusterConfig, HiveJob, InstanceGroupConfig, Job, JobControllerClient, JobControllerSettings, JobPlacement, LifecycleConfig, QueryList, SoftwareConfig, SparkJob}
import com.google.protobuf.Duration
import etlflow.utils.Executor.DATAPROC
import zio.{Layer, Task, ZIO, ZLayer}
import scala.collection.JavaConverters._

object DP {

  private def submitAndWaitForJobCompletion(name: String, jobControllerClient: JobControllerClient, projectId: String,
                                            region: String, job: Job): Unit = {
    val request = jobControllerClient.submitJob(projectId, region, job)
    val jobId = request.getReference.getJobId
    gcp_logger.info(s"Submitted job $name $jobId")
    var continue = true
    var jobInfo = jobControllerClient.getJob(projectId, region, jobId)
    var jobState = jobInfo.getStatus.getState.toString
    while (continue) {
      jobInfo = jobControllerClient.getJob(projectId, region, jobId)
      jobState = jobInfo.getStatus.getState.toString
      gcp_logger.info(s"Job $name $jobId Status $jobState")
      jobInfo.getStatus.getState.toString match {
        case "DONE" =>
          gcp_logger.info(s"Job $name $jobId completed successfully with state $jobState")
          jobControllerClient.close()
          continue = false
        case "CANCELLED" | "ERROR" =>
          val error = jobInfo.getStatus.getDetails
          gcp_logger.error(s"Job $name $jobId failed with error $error")
          jobControllerClient.close()
          throw new RuntimeException(s"Job $name failed with error $error")
        case _ =>
          TimeUnit.SECONDS.sleep(10)
      }
    }
  }

  def live(config: DATAPROC): Layer[Throwable, DPService] = ZLayer.fromEffect {
    Task {
      new DPService.Service {
        override def executeSparkJob(
             name: String, properties: Map[String, String],
              main_class: String, libs: List[String]): ZIO[DPService, Throwable, Unit] = Task {
          gcp_logger.info(s"""Trying to submit spark job $name on Dataproc with Configurations:
                             |dp_region => ${config.region}
                             |dp_project => ${config.project}
                             |dp_endpoint => ${config.endpoint}
                             |dp_cluster_name => ${config.cluster_name}
                             |main_class => $main_class
                             |properties => $properties""".stripMargin)
          val jobControllerSettings = JobControllerSettings.newBuilder().setEndpoint(config.endpoint).build()
          val jobControllerClient = JobControllerClient.create(jobControllerSettings)
          val jobPlacement = JobPlacement.newBuilder().setClusterName(config.cluster_name).build()
          val props = properties.map(x => s"${x._1}=${x._2}").mkString(",")
          val args = if (props != "")
            List("run_job", "--job_name", name, "--props", props)
          else
            List("run_job", "--job_name", name)

          gcp_logger.info("dp_libs")
          libs.foreach(gcp_logger.info)
          val sparkJob = SparkJob.newBuilder()
            .addAllJarFileUris(libs.asJava)
            .setMainClass(main_class)
            .addAllArgs(args.asJava)
            .build()
          val job: Job = Job.newBuilder().setPlacement(jobPlacement).setSparkJob(sparkJob).build()
          submitAndWaitForJobCompletion(name, jobControllerClient, config.project, config.region, job)
        }

        override def executeHiveJob(query: String): ZIO[DPService, Throwable, Unit] = Task {
          gcp_logger.info(s"""Trying to submit hive job on Dataproc with Configurations:
                             |dp_region => ${config.region}
                             |dp_project => ${config.project}
                             |dp_endpoint => ${config.endpoint}
                             |dp_cluster_name => ${config.cluster_name}
                             |query => $query""".stripMargin)
          val jobControllerSettings = JobControllerSettings.newBuilder().setEndpoint(config.endpoint).build()
          val jobControllerClient = JobControllerClient.create(jobControllerSettings)
          val jobPlacement = JobPlacement.newBuilder().setClusterName(config.cluster_name).build()
          val queryList = QueryList.newBuilder().addQueries(query)
          val hiveJob = HiveJob.newBuilder()
            .setQueryList(queryList)
            .build()
          val job = Job.newBuilder().setPlacement(jobPlacement).setHiveJob(hiveJob).build()
          submitAndWaitForJobCompletion("",jobControllerClient, config.project, config.region, job)
        }

        override def deleteDataproc(): ZIO[DPService, Throwable, Unit] = Task {
          val cluster_controller_settings = ClusterControllerSettings.newBuilder.setEndpoint(config.endpoint).build
          val cluster_controller_client = ClusterControllerClient.create(cluster_controller_settings)

          try {
            val delete_cluster_async_request = cluster_controller_client.deleteClusterAsync(config.project, config.region, config.cluster_name)
            val response = delete_cluster_async_request.get
            gcp_logger.info(s"Cluster ${config.cluster_name} successfully deleted. API response is ${response.toString}")
          } catch {
            case e: Throwable =>
              gcp_logger.error(s"Error executing deleteCluster: ${e.getMessage} ")
              throw e
          } finally if (cluster_controller_client != null) cluster_controller_client.close()
        }

        override def createDataproc(props: DataprocProperties): ZIO[DPService, Throwable, Unit] = Task {
          val end_point_config = EndpointConfig.newBuilder().setEnableHttpPortAccess(true)
          val cluster_controller_settings = ClusterControllerSettings.newBuilder.setEndpoint(config.endpoint).build
          val cluster_controller_client = ClusterControllerClient.create(cluster_controller_settings)
          val software_config = SoftwareConfig.newBuilder().setImageVersion(props.image_version)
          val disk_config_m = DiskConfig.newBuilder().setBootDiskType(props.boot_disk_type).setBootDiskSizeGb(props.master_boot_disk_size_gb)
          val disk_config_w = DiskConfig.newBuilder().setBootDiskType(props.boot_disk_type).setBootDiskSizeGb(props.worker_boot_disk_size_gb)

          val gce_cluster_builder = props.subnet_uri match {
            case Some(value) => GceClusterConfig.newBuilder()
              .setInternalIpOnly(true)
              .setSubnetworkUri(value)
              .addAllTags(props.network_tags.asJava)
              .addServiceAccountScopes("https://www.googleapis.com/auth/cloud-platform")
            case None => GceClusterConfig.newBuilder()
              .setInternalIpOnly(true)
              .addAllTags(props.network_tags.asJava)
              .addServiceAccountScopes("https://www.googleapis.com/auth/cloud-platform")
          }

          val gce_cluster_config = props.service_account match {
            case Some(value) => gce_cluster_builder.setServiceAccount(value)
            case _ => gce_cluster_builder
          }

          try {
            val master_config = InstanceGroupConfig.newBuilder.setMachineTypeUri(props.master_machine_type_uri).setNumInstances(props.master_num_instance).setDiskConfig(disk_config_m).build
            val worker_config = InstanceGroupConfig.newBuilder.setMachineTypeUri(props.worker_machine_type_uri).setNumInstances(props.worker_num_instance).setDiskConfig(disk_config_w).build
            val cluster_config_builder = ClusterConfig.newBuilder
              .setMasterConfig(master_config)
              .setWorkerConfig(worker_config)
              .setSoftwareConfig(software_config)
              .setConfigBucket(props.bucket_name)
              .setGceClusterConfig(gce_cluster_config)
              .setEndpointConfig(end_point_config)

            val cluster_config = props.idle_deletion_duration_sec match {
              case Some(value) => cluster_config_builder.setLifecycleConfig(
                LifecycleConfig.newBuilder().setIdleDeleteTtl(Duration.newBuilder().setSeconds(value))
              ).build
              case _       => cluster_config_builder.build
            }

            val cluster = Cluster.newBuilder.setClusterName(config.cluster_name).setConfig(cluster_config).build
            val create_cluster_async_request = cluster_controller_client.createClusterAsync(config.project, config.region, cluster)
            val response = create_cluster_async_request.get
            gcp_logger.info(s"Cluster created successfully: ${response.getClusterName}")
          } catch {
            case e: Throwable =>
              gcp_logger.error(s"Error creating cluster: ${e.getMessage} ")
              throw e
          } finally if (cluster_controller_client != null) cluster_controller_client.close()
        }
      }
    }
  }
}
