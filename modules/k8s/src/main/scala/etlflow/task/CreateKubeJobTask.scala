package etlflow.task

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.model.batch.v1.{Job, JobSpec}
import com.coralogix.zio.k8s.model.core.v1._
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
  *   Environment variables to pass to container, defaults to empty map
  * @param secret
  *   Secret name(secretName) which will get mounted to /etc/secretName, defaults to None
  * @param podRestartPolicy
  *   pod restart policy, defaults to 'OnFailure'
  * @param command
  *   entrypoint array, defaults to docker image's ENTRYPOINT
  * @param namespace
  *   kubernetes cluster namespace, defaults to default namespace
  */
@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
case class CreateKubeJobTask(
    name: String,
    image: String,
    imagePullPolicy: String = "IfNotPresent",
    envVars: Map[String, String] = Map.empty[String, String],
    secret: Option[String] = None,
    podRestartPolicy: String = "OnFailure",
    command: Option[Vector[String]] = None,
    namespace: K8sNamespace = K8sNamespace.default
) extends EtlTask[K8S with Jobs, Job] {

  override protected def process: RIO[K8S with Jobs, Job] = {
    logger.info("#" * 50)
    logger.info(s"Creating K8S Job: $name")

    val metadata = ObjectMeta(name = name)

    val (volumeMounts, volumes) = if (secret.isDefined) {
      val secretName           = secret.get
      val serviceAccountSecret = SecretVolumeSource(secretName = secretName, optional = false)
      val secretVolume         = Volume(name = secretName, secret = serviceAccountSecret)
      val volumeMounts: Option[Vector[VolumeMount]] = Some(
        Vector(VolumeMount(mountPath = s"/etc/$secretName", name = secretName, readOnly = true))
      )
      val volumes: Option[Vector[Volume]] = Some(Vector(secretVolume))
      (volumeMounts, volumes)
    } else { (None, None) }

    val container = Container(
      name = name,
      image = image,
      imagePullPolicy = imagePullPolicy,
      command = command,
      env = envVars.map { case (key, value) => EnvVar(name = key, value = value) }.toVector,
      volumeMounts = volumeMounts
    )

    val podSpec = PodSpec(containers = Some(Vector(container)), restartPolicy = Some(podRestartPolicy), volumes = volumes)

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
    "secrets"          -> secret.getOrElse(""),
    "envVars"          -> envVars.mkString(","),
    "podRestartPolicy" -> podRestartPolicy,
    "command"          -> command.toString
  )
}
