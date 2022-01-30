package etlflow.gcp

import com.google.cloud.dataproc.v1._
import etlflow.model.Credential.GCP
import etlflow.model.Executor.DATAPROC
import zio.{Managed, Task, TaskLayer}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

case class DPJob(client: JobControllerClient) extends DPJobApi.Service[Task] {

  private def submitAndWait(projectId: String, region: String, job: Job): Unit = {
    val request = client.submitJob(projectId, region, job)
    val jobId   = request.getReference.getJobId
    logger.info(s"Submitted job $jobId")
    var continue = true
    var jobInfo  = client.getJob(projectId, region, jobId)
    var jobState = jobInfo.getStatus.getState.toString
    while (continue) {
      jobInfo = client.getJob(projectId, region, jobId)
      jobState = jobInfo.getStatus.getState.toString
      logger.info(s"Job $jobId Status $jobState")
      jobInfo.getStatus.getState.toString match {
        case "DONE" =>
          logger.info(s"Job $jobId completed successfully with state $jobState")
          continue = false
        case "CANCELLED" | "ERROR" =>
          val error = jobInfo.getStatus.getDetails
          logger.error(s"Job $jobId failed with error $error")
          throw new RuntimeException(s"Job failed with error $error")
        case _ =>
          TimeUnit.SECONDS.sleep(10)
      }
    }
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

    val jobPlacement = JobPlacement.newBuilder().setClusterName(config.cluster_name).build()
    val spark_conf   = config.sp.map(x => (x.key, x.value)).toMap
    val sparkJob = SparkJob
      .newBuilder()
      .addAllJarFileUris(libs.asJava)
      .putAllProperties(spark_conf.asJava)
      .setMainClass(main_class)
      .addAllArgs(args.asJava)
      .build()
    val job: Job = Job.newBuilder().setPlacement(jobPlacement).setSparkJob(sparkJob).build()
    submitAndWait(config.project, config.region, job)
  }

  def executeHiveJob(query: String, config: DATAPROC): Task[Unit] = Task {
    logger.info(s"""Trying to submit hive job on Dataproc with Configurations:
                   |dp_region => ${config.region}
                   |dp_project => ${config.project}
                   |dp_endpoint => ${config.endpoint}
                   |dp_cluster_name => ${config.cluster_name}
                   |query => $query""".stripMargin)
    val jobPlacement = JobPlacement.newBuilder().setClusterName(config.cluster_name).build()
    val queryList    = QueryList.newBuilder().addQueries(query)
    val hiveJob      = HiveJob.newBuilder().setQueryList(queryList).build()
    val job          = Job.newBuilder().setPlacement(jobPlacement).setHiveJob(hiveJob).build()
    submitAndWait(config.project, config.region, job)
  }
}

object DPJob {
  def live(endpoint: String, credentials: Option[GCP] = None): TaskLayer[DPJobEnv] =
    Managed.fromAutoCloseable(Task(DPJobClient(endpoint, credentials))).map(dp => DPJob(dp)).toLayer
}
