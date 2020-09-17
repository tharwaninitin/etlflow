package etlflow.scheduler

import java.io.File

import caliban.CalibanError.ExecutionError
import com.goyeau.kubernetes.client.{KubeConfig, KubernetesClient}
import doobie.hikari.HikariTransactor
import etlflow.etljobs.{EtlJob => EtlFlowEtlJob}
import etlflow.scheduler.api.EtlFlowHelper._
import etlflow.scheduler.db.Update
import etlflow.scheduler.util.ExecutorHelper
import etlflow.utils.Executor.{DATAPROC, KUBERNETES, LOCAL_SUBPROCESS}
import etlflow.utils.{Config, JsonJackson, UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.k8s.api.batch.v1.{Job, JobSpec}
import io.k8s.api.core.v1.{Container, EnvVar, PodSpec, PodTemplateSpec}
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import zio._
import zio.blocking.blocking
import zio.interop.catz._

import scala.reflect.runtime.universe.TypeTag

abstract class Executor[EJN1 <: EtlJobName[EJP1] : TypeTag, EJP1 <: EtlJobProps : TypeTag] extends WebServer[EJN1,EJP1] with ExecutorHelper {

  val main_class: String
  val dp_libs: List[String]

  def toEtlJob(job_name: EJN1): (EJP1,Config) => EtlFlowEtlJob

  final override def runEtlJobLocal(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[EtlJob] = {
    for {
      etlJob     <- validateJob(args, etl_job_name_package)
      job_name   = UF.getEtlJobName[EJN1](etlJob.name, etl_job_name_package)
      props_map  = args.props.map(x => (x.key,x.value)).toMap
      etl_job    = toEtlJob(job_name)(job_name.getActualProperties(props_map),app_config)
      _ <- blocking(etl_job.execute()).provideLayer(ZEnv.live).foldM(
        ex => Update.updateFailedJob(job_name.toString,transactor),
        _  => Update.updateSuccessJob(job_name.toString,transactor)
      ).forkDaemon
    } yield etlJob
  }

  final override def runEtlJobLocalSubProcess(args: EtlJobArgs, transactor: HikariTransactor[Task],config: LOCAL_SUBPROCESS): Task[EtlJob] = {
    runLocalSubProcessJob(args, transactor, etl_job_name_package,config)
  }

  final override def runEtlJobDataProc(args: EtlJobArgs, transactor: HikariTransactor[Task], config: DATAPROC): Task[EtlJob] = {
    runDataprocJob(args, transactor, config, main_class, dp_libs, etl_job_name_package)
  }

  final override def runEtlJobKubernetes(args: EtlJobArgs, transactor: HikariTransactor[Task], config: KUBERNETES): Task[EtlJob] = {
    val etlJobDetails: Task[(EJN, Map[String, String])] = Task {
      val job_name      = UF.getEtlJobName[EJN](args.name, etl_job_name_package)
      val props_map     = args.props.map(x => (x.key,x.value)).toMap
      (job_name,props_map)
    }.mapError{ e =>
      println(e.getMessage)
      ExecutionError(e.getMessage)
    }

    implicit val logger = Slf4jLogger.getLogger[Task]

    val kubernetesClient = KubernetesClient[Task](KubeConfig.fromFile[Task](
      new File(s"${System.getProperty("user.home")}/.kube/config"))
    ).toManagedZIO

    val props = args.props.map(x => s"${x.key}=${x.value}").mkString(",")

    val jobParams = Seq("run_job", "--job_name", args.name, "--props", props)
    logger.info("jobParams :" + jobParams)

    val job = Job(
      metadata = Option(ObjectMeta(name = Option(args.name.toLowerCase()), labels = Option(Map("app" -> args.name.toLowerCase())))),
      spec = Option(
        JobSpec(
          template = PodTemplateSpec(
            metadata = Option(ObjectMeta(name = Option(args.name.toLowerCase()))),
            spec = Option(
              PodSpec(containers = Seq(
                Container(config.containerName.get,
                  command = Option(Seq(config.entryPoint.get)),
                  image = Option(config.imageName),
                  args = Option(
                    jobParams
                  ),
                  env = Option(Seq(
                    EnvVar("GOOGLE_APPLICATION_CREDENTIALS",config.envVar("GOOGLE_APPLICATION_CREDENTIALS")),
                    EnvVar("LOG_DB_URL",Some(app_config.dbLog.url)),
                    EnvVar("LOG_DB_USER",Some(app_config.dbLog.user)),
                    EnvVar("LOG_DB_PWD",Some(app_config.dbLog.password)),
                    EnvVar("LOG_DB_DRIVER",Some(app_config.dbLog.driver))
                  ))
                )
              ),
                restartPolicy = Option(config.restartPolicy.get))
            )
          )
        )
      )
    )
    val status = kubernetesClient.use{ client =>
      client.jobs.namespace(config.nameSpace).create(job)
    }

    for {
      (job_name,props_map) <- etlJobDetails
      execution_props  <- Task {
        JsonJackson.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
          .map(x => (x._1, x._2.toString))
      }.mapError{ e =>
        println(e.getMessage)
        ExecutionError(e.getMessage)
      }
      _  <- status.foldM(
        ex => UIO(println(ex.getMessage)) *> Update.updateFailedJob(job_name.toString,transactor),
        op  => Update.updateSuccessJob(job_name.toString,transactor)
      ).forkDaemon
    } yield EtlJob(args.name,execution_props)
  }
}