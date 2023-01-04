package etlflow

import io.kubernetes.client.openapi.models.V1Job

package object k8s {

  type Jobs = K8S[V1Job]

  sealed trait DeletionPolicy {
    override def toString: String = getClass.getSimpleName.init // init drop the last $
  }

  sealed trait JobStatus {
    override def toString: String = getClass.getSimpleName.init // init drop the last $
  }

  object DeletionPolicy {
    case object OnComplete extends DeletionPolicy
    case object OnSuccess  extends DeletionPolicy
    case object OnFailure  extends DeletionPolicy
    case object Never      extends DeletionPolicy
  }

  object JobStatus {
    case object Running extends JobStatus
    case object Succeed extends JobStatus
    case object Failure extends JobStatus
  }
}
