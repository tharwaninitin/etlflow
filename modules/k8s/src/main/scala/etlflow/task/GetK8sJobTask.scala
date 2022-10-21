package etlflow.task

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.model.batch.v1.Job
import etlflow.k8s._
import zio.RIO

case class GetK8sJobTask(name: String) extends EtlTask[K8sEnv with Jobs, Job] {

  override protected def process: RIO[K8sEnv with Jobs, Job] = {
    logger.info("#" * 100)
    logger.info(s"Getting K8S Job Detail Task: $name")
    K8sApi.getJob(name = name)
  }

  override def getTaskProperties: Map[String, String] = Map("name" -> name)
}
