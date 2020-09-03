package etlflow.gcp

import java.util.concurrent.TimeUnit
import com.google.cloud.dataproc.v1.{HiveJob, Job, JobControllerClient, JobControllerSettings, JobPlacement, QueryList, SparkJob}
import etlflow.utils.Executor.DATAPROC
import zio.{Layer, Task, ZIO, ZLayer}
import scala.collection.JavaConverters._
import zio.blocking._

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
      }
    }
  }
}
