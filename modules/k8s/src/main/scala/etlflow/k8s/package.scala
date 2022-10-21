package etlflow

import com.coralogix.zio.k8s.client.batch.v1.jobs.Jobs

package object k8s {
  type K8sEnv = K8SApi.Service with Jobs
}
