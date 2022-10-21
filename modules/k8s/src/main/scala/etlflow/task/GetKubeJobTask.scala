package etlflow.task

import com.coralogix.zio.k8s.model.batch.v1.Job
import etlflow.k8s._
import zio.RIO

/** Get a kubernetes job details for given job name
  * @param name
  *   kubernetes job name
  */
case class GetKubeJobTask(name: String) extends EtlTask[K8sEnv, Job] {

  override protected def process: RIO[K8sEnv, Job] = {
    logger.info("#" * 100)
    logger.info(s"Getting K8S Job Detail Task: $name")
    K8SApi.getJob(name = name)
  }

  override def getTaskProperties: Map[String, String] = Map("name" -> name)
}
