package etlflow.k8s

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs
import com.coralogix.zio.k8s.model.batch.v1.{Job, JobSpec}
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import zio.{RIO, ZIO}

object K8SApi {
  trait Service {

    /** Method: createJob - Submit a job to kubernetes cluster using give job configuration
      * @param metadata
      *   kubernetes job metadata
      * @param spec
      *   kubernetes job spec
      * @return
      *   Job
      */
    def createJob(metadata: ObjectMeta, spec: JobSpec): RIO[Jobs, Job]

    /** Method: getJob - Get a kubernetes job details for given job name
      * @param name
      *   kubernetes job name
      * @return
      *   Job
      */
    def getJob(name: String): RIO[Jobs, Job]
  }

  def createJob(metadata: ObjectMeta, spec: JobSpec): RIO[K8sEnv, Job] =
    ZIO.environmentWithZIO[K8SApi.Service](_.get.createJob(metadata, spec))
  def getJob(name: String): RIO[K8sEnv, Job] =
    ZIO.environmentWithZIO[K8SApi.Service](_.get.getJob(name))
}
