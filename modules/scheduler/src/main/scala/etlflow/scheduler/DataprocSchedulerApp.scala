package etlflow.scheduler

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import caliban.CalibanError.ExecutionError
import com.google.cloud.dataproc.v1.{Job, JobControllerClient, JobControllerSettings, JobPlacement, SparkJob}
import doobie.hikari.HikariTransactor
import etlflow.scheduler.EtlFlowHelper._
import etlflow.utils.{GlobalProperties, UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import zio._
import scala.reflect.runtime.universe.TypeTag

abstract class DataprocSchedulerApp[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag, EJGP <: GlobalProperties : TypeTag]
  extends SchedulerApp[EJN,EJP,EJGP] {

  val main_class: String
  val dp_libs: List[String]
  val gcp_region: String
  val gcp_project: String
  val gcp_dp_endpoint: String
  val gcp_dp_cluster_name: String

  private def executeDataProcJob(job_name: String, job_properties: Map[String,String]): Task[Unit] = Task {
    val props = job_properties.map(x => s"${x._1}=${x._2}").mkString(",")
    val args = if (props != "")
      List("run_job", "--job_name", job_name, "--props", props)
    else
      List("run_job", "--job_name", job_name)

    logger.info(s"""DataProc Configurations:
                      |gcp_region => $gcp_region
                      |gcp_project => $gcp_project
                      |gcp_dp_endpoint => $gcp_dp_endpoint
                      |gcp_dp_cluster_name => $gcp_dp_cluster_name
                      |main_class => $main_class
                      |args => $args""".stripMargin)
    logger.info("dp_libs")
    dp_libs.foreach(logger.info)

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
    logger.info(s"Submitted job $jobId")
    waitForJobCompletion(jobControllerClient, gcp_project, gcp_region, jobId)
  }.mapError{e =>
    logger.error(e.getMessage + " " + e.getStackTrace.mkString("\n"))
    e
  }
  private def waitForJobCompletion(jobControllerClient: JobControllerClient, projectId: String, region: String, jobId: String): Unit = {
    var continue = true
    var jobInfo = jobControllerClient.getJob(projectId, region, jobId)
    var jobState = jobInfo.getStatus.getState.toString
    while (continue) {
      jobInfo = jobControllerClient.getJob(projectId, region, jobId)
      jobState = jobInfo.getStatus.getState.toString
      logger.info(s"Job Status $jobState")
      jobInfo.getStatus.getState.toString match {
        case "DONE" =>
          logger.info(s"Job $jobId completed successfully with state $jobState")
          continue = false
        case "CANCELLED" | "ERROR" =>
          logger.info(s"Job $jobId failed with state $jobState")
          throw new RuntimeException("Job failed")
        case _ =>
          TimeUnit.SECONDS.sleep(2)
      }
    }
  }

  final override def runEtlJob(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[EtlJob] = {
    val etlJobDetails: Task[(EJN, Map[String, String])] = Task {
      val job_name      = UF.getEtlJobName[EJN](args.name, etl_job_name_package)
      val props_map     = args.props.map(x => (x.key,x.value)).toMap
      (job_name,props_map)
    }.mapError(e => ExecutionError(e.getMessage))

    for {
      (job_name,props_map) <- etlJobDetails
      execution_props  <- Task {
                          UF.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
                            .map(x => (x._1, x._2.toString))
                        }.mapError{ e =>
                          logger.error(e.getMessage)
                          ExecutionError(e.getMessage)
                        }
      _  <- executeDataProcJob(job_name.toString,Map.empty).foldM(
                ex => updateFailedJob(job_name.toString,transactor),
                _  => updateSuccessJob(job_name.toString,transactor)
            ).forkDaemon
    } yield EtlJob(args.name,execution_props)
  }
}
