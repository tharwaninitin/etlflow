package etlflow.k8s

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.client.v1.pods.Pods
import com.coralogix.zio.k8s.model.batch.v1.{Job, JobSpec}
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import zio.{RIO, ZIO}

object K8sApi {
  trait Service {
    def createJob(metadata: ObjectMeta, spec: JobSpec): RIO[Jobs, Job]
    def getJob(name: String): RIO[Jobs, Job]
    def getPodLog(podName: String, containerName: Option[String] = None): RIO[Pods, Unit]
  }

  def createJob(metadata: ObjectMeta, spec: JobSpec): RIO[K8sEnv with Jobs, Job] =
    ZIO.environmentWithZIO[K8sEnv](_.get.createJob(metadata, spec))
  def getJob(name: String): RIO[K8sEnv with Jobs, Job] =
    ZIO.environmentWithZIO[K8sEnv](_.get.getJob(name))
  def getPodLog(podName: String, containerName: Option[String] = None): RIO[K8sEnv with Pods, Unit] =
    ZIO.environmentWithZIO[K8sEnv](_.get.getPodLog(podName, containerName))
}
