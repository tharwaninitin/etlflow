package etlflow.task

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
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
case class GetKubeJobTask(name: String, namespace: K8sNamespace = K8sNamespace.default) extends EtlTask[K8S with Jobs, Job] {

  override protected def process: RIO[K8S with Jobs, Job] = {
    logger.info(s"Getting K8S $name Job Details")
    K8S.getJob(name, namespace)
  }

  override def getTaskProperties: Map[String, String] = Map("name" -> name)
}
