package etlflow.k8s

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.client.config.httpclient.k8sDefault
import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.model.batch.v1.{Job, JobSpec}
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import zio.{RIO, TaskLayer, ULayer, ZIO, ZLayer}

trait K8S {

  /** Method: createJob - Submit a job to kubernetes cluster using given job configuration
    * @param metadata
    *   kubernetes job metadata
    * @param spec
    *   kubernetes job spec
    * @param namespace
    *   kubernetes cluster namespace
    * @return
    *   Job
    */
  def createJob(metadata: ObjectMeta, spec: JobSpec, namespace: K8sNamespace): RIO[Jobs, Job]

  /** Method: getJob - Get a kubernetes job details for given job name
    * @param name
    *   kubernetes job name
    * @param namespace
    *   kubernetes cluster namespace
    * @return
    *   Job
    */
  def getJob(name: String, namespace: K8sNamespace): RIO[Jobs, Job]
}
object K8S {

  /** Method: createJob - Submit a job to kubernetes cluster using given job configuration
    * @param metadata
    *   kubernetes job metadata
    * @param spec
    *   kubernetes job spec
    * @param namespace
    *   kubernetes cluster namespace, defaults to default namespace
    * @return
    *   Job
    */
  def createJob(metadata: ObjectMeta, spec: JobSpec, namespace: K8sNamespace = K8sNamespace.default): RIO[K8S with Jobs, Job] =
    ZIO.environmentWithZIO[K8S](_.get.createJob(metadata, spec, namespace))

  /** Method: getJob - Get a kubernetes job details for given job name
    * @param name
    *   kubernetes job name
    * @param namespace
    *   kubernetes cluster namespace, defaults to default namespace
    * @return
    *   Job
    */
  def getJob(name: String, namespace: K8sNamespace = K8sNamespace.default): RIO[K8S with Jobs, Job] =
    ZIO.environmentWithZIO[K8S](_.get.getJob(name, namespace))

  val live: TaskLayer[K8S with Jobs] = (k8sDefault >>> Jobs.live) ++ ZLayer.succeed(K8SImpl())

  val test: ULayer[K8S with Jobs] = Jobs.test ++ ZLayer.succeed(K8SImpl())
}
