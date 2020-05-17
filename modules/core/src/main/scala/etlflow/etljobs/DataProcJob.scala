package etlflow.etljobs

import java.util.concurrent.TimeUnit
import com.google.cloud.dataproc.v1._
import etlflow.utils.GlobalProperties
import org.apache.log4j.Logger
import scala.collection.JavaConverters._

trait DataProcJob[EJGP <: GlobalProperties] {

  lazy val dp_logger: Logger = Logger.getLogger(getClass.getName)

  val main_class: String = getClass.getName.replace("$","")
  val dp_libs: List[String] = List.empty

  def executeDataProcJob(job_name: String, job_properties: Map[String,String],global_properties: Option[EJGP]): Unit = {
    val gcp_region: String = global_properties.map(x => x.gcp_region).getOrElse("<not_set>")
    val gcp_project: String = global_properties.map(x => x.gcp_project).getOrElse("<not_set>")
    val gcp_dp_endpoint: String = global_properties.map(x => x.gcp_dp_endpoint).getOrElse("<not_set>")
    val gcp_dp_cluster_name: String = global_properties.map(x => x.gcp_dp_cluster_name).getOrElse("<not_set>")

    val props = job_properties.map(x => s"${x._1}=${x._2}").mkString(",")
    val args =
      if (props != "")
        List("run_job", "--job_name", job_name, "--props", props)
      else
        List("run_job", "--job_name", job_name)

    dp_logger.info(s"""gcp_region => $gcp_region
         |gcp_project => $gcp_project
         |gcp_dp_endpoint => $gcp_dp_endpoint
         |gcp_dp_cluster_name => $gcp_dp_cluster_name
         |main_class => $main_class
         |args => $args""".stripMargin)
    dp_logger.info("dp_libs")
    dp_libs.foreach(dp_logger.info(_))

    val jobControllerSettings = JobControllerSettings.newBuilder().setEndpoint(gcp_dp_endpoint).build()
    val jobControllerClient = JobControllerClient.create(jobControllerSettings)
    val jobPlacement = JobPlacement.newBuilder().setClusterName(gcp_dp_cluster_name).build()
    val sparkJob = SparkJob.newBuilder()
      .addAllJarFileUris(dp_libs.asJava)
      .setMainClass(main_class)
      .addAllArgs(args.asJava)
      .build()
    val job = Job.newBuilder().setPlacement(jobPlacement).setSparkJob(sparkJob).build()
    val request = jobControllerClient.submitJob(gcp_project, gcp_region, job)
    val jobId = request.getReference.getJobId
    dp_logger.info(s"Submitted job $jobId")
    waitForJobCompletion(jobControllerClient, gcp_project, gcp_region, jobId)
  }

  private def waitForJobCompletion(jobControllerClient: JobControllerClient, projectId: String, region: String, jobId: String): Unit = {
    var continue = true
    var jobInfo = jobControllerClient.getJob(projectId, region, jobId)
    var jobState = jobInfo.getStatus.getState.toString
    while (continue) {
      jobInfo = jobControllerClient.getJob(projectId, region, jobId)
      jobState = jobInfo.getStatus.getState.toString
      dp_logger.info(s"Job Status $jobState")
      jobInfo.getStatus.getState.toString match {
        case "DONE" =>
          dp_logger.info(s"Job $jobId completed successfully with state $jobState")
          continue = false
        case "CANCELLED" | "ERROR" =>
          dp_logger.info(s"Job $jobId failed with state $jobState")
          throw new RuntimeException("Job failed")
        case _ =>
          TimeUnit.SECONDS.sleep(2)
      }
    }
  }
}
