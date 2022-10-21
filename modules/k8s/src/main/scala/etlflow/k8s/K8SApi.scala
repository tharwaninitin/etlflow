package etlflow.k8s

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.model.batch.v1.{Job, JobSpec}
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import zio.{RIO, ZIO}

object K8SApi {
  trait Service {

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
  def createJob(metadata: ObjectMeta, spec: JobSpec, namespace: K8sNamespace = K8sNamespace.default): RIO[K8sEnv, Job] =
    ZIO.environmentWithZIO[K8SApi.Service](_.get.createJob(metadata, spec, namespace))

  /** Method: getJob - Get a kubernetes job details for given job name
    * @param name
    *   kubernetes job name
    * @param namespace
    *   kubernetes cluster namespace, defaults to default namespace
    * @return
    *   Job
    */
  def getJob(name: String, namespace: K8sNamespace = K8sNamespace.default): RIO[K8sEnv, Job] =
    ZIO.environmentWithZIO[K8SApi.Service](_.get.getJob(name, namespace))
}
