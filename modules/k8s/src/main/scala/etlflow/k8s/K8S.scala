package etlflow.k8s

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.client.config.httpclient.k8sDefault
import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.model.batch.v1.{Job, JobSpec}
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import zio.{RIO, TaskLayer, ZIO, ZLayer}

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
case class K8S() extends K8SApi.Service {

  /** Method: createJob - Submit a job to kubernetes cluster using give job configuration
    * @param metadata
    *   kubernetes job metadata
    * @param spec
    *   kubernetes job spec
    * @return
    *   Job
    */
  def createJob(metadata: ObjectMeta, spec: JobSpec): RIO[Jobs, Job] =
    ZIO
      .environmentWithZIO[Jobs](_.get.create(Job(metadata = Some(metadata), spec = Some(spec)), K8sNamespace.default))
      .mapError(e => new Exception(e.toString))

  /** Method: getJob - Get a kubernetes job details for given job name
    * @param name
    *   kubernetes job name
    * @return
    *   Job
    */
  def getJob(name: String): RIO[Jobs, Job] =
    ZIO
      .environmentWithZIO[Jobs](_.get.get(name, K8sNamespace.default))
      .mapError(e => new Exception(e.toString))

}

object K8S {
  val live: TaskLayer[K8sEnv] = (k8sDefault >>> Jobs.live) ++ ZLayer.succeed(K8S())
}
