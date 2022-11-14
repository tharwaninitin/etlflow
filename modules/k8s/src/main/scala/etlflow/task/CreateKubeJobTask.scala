package etlflow.task

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.model.batch.v1.{Job, JobSpec}
import com.coralogix.zio.k8s.model.core.v1.{Container, EnvVar, PodSpec, PodTemplateSpec}
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import etlflow.k8s._
import zio.{RIO, ZIO}

/** Submit a job to kubernetes cluster using give job configuration
  * @param name
  *   kubernetes job name
  * @param image
  *   docker image name
  * @param imagePullPolicy
  *   docker image pull policy, default to 'IfNotPresent'
  * @param envVars
  *   Environment variables to pass to container, default to empty map
  * @param podRestartPolicy
  *   pod restart policy, default to 'OnFailure'
  * @param command
  *   entrypoint array, default to docker image's ENTRYPOINT
  * @param namespace
  *   kubernetes cluster namespace, defaults to default namespace
  */
case class CreateKubeJobTask(
    name: String,
    image: String,
    imagePullPolicy: String = "IfNotPresent",
    envVars: Map[String, String] = Map.empty[String, String],
    podRestartPolicy: String = "OnFailure",
    command: Option[Vector[String]] = None,
    namespace: K8sNamespace = K8sNamespace.default
) extends EtlTask[K8S with Jobs, Job] {

  override protected def process: RIO[K8S with Jobs, Job] = {
    logger.info("#" * 50)
    logger.info(s"Creating K8S Job: $name")

    val metadata = ObjectMeta(name = name)

    val container = Container(
      name = name,
      image = image,
      imagePullPolicy = imagePullPolicy,
      command = command,
      env = envVars.map { case (key, value) => EnvVar(name = key, value = value) }.toVector
    )

    val podSpec = PodSpec(containers = Some(Vector(container)), restartPolicy = Some(podRestartPolicy))

    val podTemplateSpec = PodTemplateSpec(metadata = Some(metadata), spec = Some(podSpec))

    K8S
      .createJob(metadata, JobSpec(template = podTemplateSpec), namespace)
      .tapError(e => ZIO.logError(e.getMessage))
      .zipLeft(ZIO.logInfo(s"K8S Job $name submitted successfully"))
      .zipLeft(ZIO.logInfo("#" * 50))
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getTaskProperties: Map[String, String] = Map(
    "name"             -> name,
    "image"            -> image,
    "imagePullPolicy"  -> imagePullPolicy,
    "envVars"          -> envVars.mkString(","),
    "podRestartPolicy" -> podRestartPolicy,
    "command"          -> command.toString
  )
}
