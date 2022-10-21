package etlflow.task

import com.coralogix.zio.k8s.client.v1.pods.Pods
import etlflow.k8s._
import zio.RIO

case class GetK8sPodLogTask(name: String, containerName: Option[String] = None) extends EtlTask[K8sEnv with Pods, Unit] {

  override protected def process: RIO[K8sEnv with Pods, Unit] = {
    logger.info("#" * 100)
    logger.info(s"Getting K8S Pod Logs Task: $name")
    K8sApi.getPodLog(podName = name, containerName = containerName)
  }

  override def getTaskProperties: Map[String, String] = Map(
    "name"          -> name,
    "containerName" -> containerName.getOrElse("")
  )
}
