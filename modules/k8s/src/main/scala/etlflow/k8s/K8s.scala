package etlflow.k8s

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.client.config.httpclient.k8sDefault
import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.client.v1.pods.Pods
import com.coralogix.zio.k8s.model.batch.v1.{Job, JobSpec}
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import zio.stream.ZStream
import zio.{RIO, TaskLayer, ZIO, ZLayer}

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
case class K8s() extends K8sApi.Service {
  def createJob(metadata: ObjectMeta, spec: JobSpec): RIO[Jobs, Job] =
    ZIO
      .environmentWithZIO[Jobs](_.get.create(Job(metadata = Some(metadata), spec = Some(spec)), K8sNamespace.default))
      .mapError(e => new Exception(e.toString))

  def getJob(name: String): RIO[Jobs, Job] =
    ZIO
      .environmentWithZIO[Jobs](_.get.get(name, K8sNamespace.default))
      .mapError(e => new Exception(e.toString))

  def getPodLog(podName: String, containerName: Option[String] = None): RIO[Pods, Unit] =
    ZStream
      .environmentWithStream[Pods](_.get.getLog(podName, K8sNamespace.default, container = containerName, follow = Some(true)))
      .tap(line => ZIO.logInfo(line))
      .runDrain
      .mapError(e => new Exception(e.toString))

}

object K8s {
  val live: TaskLayer[K8sEnv with Jobs with Pods] = (k8sDefault >>> (Jobs.live ++ Pods.live)) ++ ZLayer.succeed(K8s())
}
