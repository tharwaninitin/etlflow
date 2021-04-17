package etlflow.executor

import java.io.File
import com.goyeau.kubernetes.client.{KubeConfig, KubernetesClient}
import etlflow.api.Schema.EtlJobArgs
import etlflow.utils.Executor.KUBERNETES
import etlflow.Credential.JDBC
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.k8s.api.batch.v1.{Job, JobSpec}
import io.k8s.api.core.v1.{Container, EnvVar, PodSpec, PodTemplateSpec}
import io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
import org.http4s.Status
import zio.Task
import zio.interop.catz._

trait K8SExecutor {

  def runK8sJob(args: EtlJobArgs, db: JDBC, config: KUBERNETES): Task[Status] = Task.concurrentEffectWith { implicit CE =>

    implicit val logger = Slf4jLogger.getLogger[Task]

    val kubernetesClient = KubernetesClient[Task](KubeConfig.fromFile[Task](
      new File(s"${System.getProperty("user.home")}/.kube/config"))
    ).toManagedZIO

    val props = args.props.getOrElse(List.empty).map(x => s"${x.key}=${x.value}").mkString(",")

    val jobParams = Seq("run_job", "--job_name", args.name, "--props", props)
    logger.info("jobParams :" + jobParams)

    val job = Job(
      metadata = Option(ObjectMeta(name = Option(args.name.toLowerCase()), labels = Option(Map("app" -> args.name.toLowerCase())))),
      spec = Option(
        JobSpec(
          template = PodTemplateSpec(
            metadata = Option(ObjectMeta(name = Option(args.name.toLowerCase()))),
            spec = Option(
              PodSpec(
                containers = Seq(
                  Container(
                    config.containerName,
                    command = config.entryPoint.flatMap(ep => Option(Seq(ep))),
                    image = Option(config.imageName),
                    args = Option(jobParams),
                    env = Option(Seq(
                      EnvVar("GOOGLE_APPLICATION_CREDENTIALS", config.envVar("GOOGLE_APPLICATION_CREDENTIALS")),
                      EnvVar("LOG_DB_URL", Some(db.url)),
                      EnvVar("LOG_DB_USER", Some(db.user)),
                      EnvVar("LOG_DB_PWD", Some(db.password)),
                      EnvVar("LOG_DB_DRIVER", Some(db.driver))
                    ))
                  )
                ),
                restartPolicy = config.restartPolicy
              )
            )
          )
        )
      )
    )

    kubernetesClient.use { client =>
      client.jobs.namespace(config.nameSpace).create(job)
    }
  }

}
