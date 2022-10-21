package etlflow.task

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.model.batch.v1.{Job, JobSpec}
import com.coralogix.zio.k8s.model.core.v1.{Container, PodSpec, PodTemplateSpec}
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import etlflow.k8s._
import zio.RIO

case class CreateK8sJobTask(
    name: String,
    image: String,
    imagePullPolicy: String = "IfNotPresent",
    podRestartPolicy: String = "OnFailure",
    command: Option[Vector[String]] = None
) extends EtlTask[K8sEnv with Jobs, Job] {

  override protected def process: RIO[K8sEnv with Jobs, Job] = {
    logger.info("#" * 100)
    logger.info(s"Creating K8S Job Task: $name")

    val metadata        = ObjectMeta(name = Some(name))
    val container       = Container(name = name, image = Some(image), imagePullPolicy = Some(imagePullPolicy), command = command)
    val podSpec         = PodSpec(containers = Some(Vector(container)), restartPolicy = Some(podRestartPolicy))
    val podTemplateSpec = PodTemplateSpec(metadata = Some(metadata), spec = Some(podSpec))

    val job = K8sApi.createJob(metadata = metadata, spec = JobSpec(template = podTemplateSpec))
    logger.info(s"K8S Job submitted successfully: $name")
    job
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getTaskProperties: Map[String, String] = Map(
    "name"             -> name,
    "image"            -> image,
    "imagePullPolicy"  -> imagePullPolicy,
    "podRestartPolicy" -> podRestartPolicy,
    "command"          -> command.toString
  )
}
