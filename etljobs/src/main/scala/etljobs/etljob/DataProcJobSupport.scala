package etljobs.etljob

import java.util.concurrent.TimeUnit
import com.google.cloud.dataproc.v1._
import org.apache.log4j.Logger
import zio.ZIO
import scala.collection.JavaConverters._

trait DataProcJobSupport {
  lazy val dp_logger: Logger = Logger.getLogger(getClass.getName)
  val region: String
  val projectId: String
  val endPoint: String
  val clusterName: String
  val mainClass: String
  val libs: List[String]

  def executeDataProcJob(job_name: String, job_properties: Map[String,String]): Unit = {
    val props = job_properties.map(x => s"${x._1}=${x._2}").mkString(",")
    val args = List(
      "run_job_remote",
      "--job_name",
      job_name,
      "--props",
      props
    )
    val jobControllerSettings = JobControllerSettings.newBuilder().setEndpoint(endPoint).build()
    val jobControllerClient = JobControllerClient.create(jobControllerSettings)
    val jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build()
    val sparkJob = SparkJob.newBuilder()
      .addAllJarFileUris(libs.asJava)
      .setMainClass(mainClass)
      .addAllArgs(args.asJava)
      .build()
    val job = Job.newBuilder().setPlacement(jobPlacement).setSparkJob(sparkJob).build()
    val request = jobControllerClient.submitJob(projectId, region, job)
    val jobId = request.getReference.getJobId
    dp_logger.info(s"Submitted job $jobId")
    waitForJobCompletion(jobControllerClient, projectId, region, jobId)
  }

  private[etljob] def waitForJobCompletion(jobControllerClient: JobControllerClient, projectId: String, region: String, jobId: String): Unit = {
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
