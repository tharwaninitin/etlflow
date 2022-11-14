package etlflow.task

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.Status
import etlflow.k8s._
import zio.RIO

/** Delete kubernetes job for given job name
  * @param name
  *   kubernetes job name
  * @param namespace
  *   kubernetes cluster namespace defaults to default namespace
  */
case class DeleteKubeJobTask(name: String, namespace: K8sNamespace = K8sNamespace.default)
    extends EtlTask[K8S with Jobs, Status] {

  override protected def process: RIO[K8S with Jobs, Status] = {
    logger.info(s"Deleting K8S $name Job")
    K8S.deleteJob(name, namespace = namespace)
  }

  override def getTaskProperties: Map[String, String] = Map("name" -> name)
}
