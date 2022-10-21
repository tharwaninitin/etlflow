package etlflow.task

import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.model.batch.v1.Job
import etlflow.k8s._
import zio.RIO

/** Get a kubernetes job details for given job name
  * @param name
  *   kubernetes job name
  * @param namespace
  *   kubernetes cluster namespace defaults to default namespace
  */
case class GetKubeJobTask(name: String, namespace: K8sNamespace = K8sNamespace.default) extends EtlTask[K8sEnv, Job] {

  override protected def process: RIO[K8sEnv, Job] = {
    logger.info(s"Getting K8S Job Details: $name")
    K8SApi.getJob(name, namespace)
  }

  override def getTaskProperties: Map[String, String] = Map("name" -> name)
}
