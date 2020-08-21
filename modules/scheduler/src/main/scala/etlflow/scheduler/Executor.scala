package etlflow.scheduler
import java.io.File
import caliban.CalibanError.ExecutionError
import com.goyeau.kubernetes.client.{KubeConfig, KubernetesClient}
import doobie.hikari.HikariTransactor
import etlflow.etljobs.{EtlJob => EtlFlowEtlJob}
import etlflow.gcp.{DP, DPService}
import etlflow.scheduler.api.EtlFlowHelper._
import etlflow.scheduler.db.Update
import etlflow.utils.Executor.{DATAPROC, KUBERNETES}
import etlflow.utils.{Config, JsonJackson, UtilityFunctions => UF}
import etlflow.{EtlJobName, EtlJobProps}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.k8s.api.batch.v1.{JobSpec,Job}
import io.k8s.api.core.v1.{Container, EnvVar, PodSpec, PodTemplateSpec}
import io.chrisdavenport.log4cats.Logger
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import org.http4s.Status
import zio._
import zio.blocking.{Blocking, blocking}
import zio.interop.catz._
import scala.reflect.runtime.universe.TypeTag
abstract class Executor[EJN <: EtlJobName[EJP] : TypeTag, EJP <: EtlJobProps : TypeTag]
  extends WebServer[EJN,EJP] {
  val main_class: String
  val dp_libs: List[String]
  def toEtlJob(job_name: EJN): (EJP,Config) => EtlFlowEtlJob

  final override def runEtlJobDataProc(args: EtlJobArgs, transactor: HikariTransactor[Task], config: DATAPROC): Task[EtlJob] = {
    val etlJobDetails: Task[(EJN, Map[String, String])] = Task {
      val job_name      = UF.getEtlJobName[EJN](args.name, etl_job_name_package)
      val props_map     = args.props.map(x => (x.key,x.value)).toMap
      (job_name,props_map)
    }.mapError{ e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
    }
    for {
      (job_name,props_map) <- etlJobDetails
      execution_props  <- Task {
        JsonJackson.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
          .map(x => (x._1, x._2.toString))
      }.mapError{ e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
      _  <- blocking(DPService.executeSparkJob(job_name.toString,props_map,main_class,dp_libs).provideLayer(DP.live(config)))
        .provideLayer(Blocking.live).foldM(
        ex => UIO(logger.error(ex.getMessage)) *> Update.updateFailedJob(job_name.toString,transactor),
        _  => Update.updateSuccessJob(job_name.toString,transactor)
      ).forkDaemon
    } yield EtlJob(args.name,execution_props)
  }
  final override def runEtlJobLocal(args: EtlJobArgs, transactor: HikariTransactor[Task]): Task[EtlJob] = {
    val etlJobDetails: Task[(EJN, EtlFlowEtlJob, Map[String, String])] = Task {
      val job_name      = UF.getEtlJobName[EJN](args.name, etl_job_name_package)
      val props_map     = args.props.map(x => (x.key,x.value)).toMap
      val etl_job       = toEtlJob(job_name)(job_name.getActualProperties(props_map),globalProperties)
      etl_job.job_name  = job_name.toString
      (job_name,etl_job,props_map)
    }.mapError{ e =>
      logger.error(e.getMessage)
      ExecutionError(e.getMessage)
    }
    for {
      (job_name,etl_job,props_map) <- etlJobDetails
      execution_props   <- Task {
        JsonJackson.convertToJsonByRemovingKeysAsMap(job_name.getActualProperties(props_map), List.empty)
          .map(x => (x._1, x._2.toString))
      }.mapError{ e =>
        logger.error(e.getMessage)
        ExecutionError(e.getMessage)
      }
      _                  <- blocking(etl_job.execute()).provideLayer(ZEnv.live).foldM(
        ex => Update.updateFailedJob(job_name.toString,transactor),
        _  => Update.updateSuccessJob(job_name.toString,transactor)
      ).forkDaemon
    } yield EtlJob(args.name,execution_props)
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

    implicit val logger: Logger[Task] = Slf4jLogger.getLogger[Task]

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
                    EnvVar("LOG_DB_URL",config.envVar("LOG_DB_URL")),
                    EnvVar("LOG_DB_USER",config.envVar("LOG_DB_USER")),
                    EnvVar("LOG_DB_PWD",config.envVar("LOG_DB_PWD")),
                    EnvVar("LOG_DB_DRIVER",config.envVar("LOG_DB_DRIVER"))
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