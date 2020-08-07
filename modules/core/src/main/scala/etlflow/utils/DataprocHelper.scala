//package etlflow.utils
//
//import java.util.concurrent.TimeUnit
//import com.google.cloud.dataproc.v1.{Job, JobControllerClient, JobControllerSettings, JobPlacement, SparkJob}
//import org.slf4j.{Logger, LoggerFactory}
//import zio.Task
//import scala.collection.JavaConverters._
//
//trait DataprocHelper {
//
//  val main_class: String
//  val dp_libs: List[String]
//  val gcp_region: String
//  val gcp_project: String
//  val gcp_dp_endpoint: String
//  val gcp_dp_cluster_name: String
//
//  val dataprocHelperLogger: Logger = LoggerFactory.getLogger(getClass.getName)
//
//  def executeDataProcJob(job_name: String, job_properties: Map[String,String]): Task[Unit] = Task {
//    val props = job_properties.map(x => s"${x._1}=${x._2}").mkString(",")
//    val args = if (props != "")
//      List("run_job", "--job_name", job_name, "--props", props)
//    else
//      List("run_job", "--job_name", job_name)
//
//    dataprocHelperLogger.info(s"""DataProc Configurations:
//                   |gcp_region => $gcp_region
//                   |gcp_project => $gcp_project
//                   |gcp_dp_endpoint => $gcp_dp_endpoint
//                   |gcp_dp_cluster_name => $gcp_dp_cluster_name
//                   |main_class => $main_class
//                   |args => $args""".stripMargin)
//    dataprocHelperLogger.info("dp_libs")
//    dp_libs.foreach(dataprocHelperLogger.info)
//
//    val jobControllerSettings = JobControllerSettings.newBuilder().setEndpoint(gcp_dp_endpoint).build()
//    val jobControllerClient = JobControllerClient.create(jobControllerSettings)
//    val jobPlacement = JobPlacement.newBuilder().setClusterName(gcp_dp_cluster_name).build()
//    val sparkJob = SparkJob.newBuilder()
//      .addAllJarFileUris(dp_libs.asJava)
//      .setMainClass(main_class)
//      .addAllArgs(args.asJava)
//      .build()
//    val job = Job.newBuilder().setPlacement(jobPlacement).setSparkJob(sparkJob).build()
//    val request = jobControllerClient.submitJob(gcp_project, gcp_region, job)
//    val jobId = request.getReference.getJobId
//    dataprocHelperLogger.info(s"Submitted job $jobId")
//    waitForJobCompletion(jobControllerClient, gcp_project, gcp_region, jobId)
//  }.mapError{e =>
//    dataprocHelperLogger.error(e.getMessage + " " + e.getStackTrace.mkString("\n"))
//    e
//  }
//
//  def waitForJobCompletion(jobControllerClient: JobControllerClient, projectId: String, region: String, jobId: String): Unit = {
//    var continue = true
//    var jobInfo = jobControllerClient.getJob(projectId, region, jobId)
//    var jobState = jobInfo.getStatus.getState.toString
//    while (continue) {
//      jobInfo = jobControllerClient.getJob(projectId, region, jobId)
//      jobState = jobInfo.getStatus.getState.toString
//      dataprocHelperLogger.info(s"Job Status $jobState")
//      jobInfo.getStatus.getState.toString match {
//        case "DONE" =>
//          dataprocHelperLogger.info(s"Job $jobId completed successfully with state $jobState")
//          continue = false
//        case "CANCELLED" | "ERROR" =>
//          dataprocHelperLogger.error(s"Job $jobId failed with error ${jobInfo.getStatus.getDetails}")
//          throw new RuntimeException("Job failed")
//        case _ =>
//          TimeUnit.SECONDS.sleep(2)
//      }
//    }
//  }
//}
