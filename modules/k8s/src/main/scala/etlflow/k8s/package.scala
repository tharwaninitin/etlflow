package etlflow

import io.kubernetes.client.openapi.models.V1Job

package object k8s {

  type Jobs = K8S[V1Job]

  sealed trait DeletionPolicy {
    override def toString: String = {
      val clazz = getClass
      val name  = clazz.getSimpleName
      if (clazz.isSynthetic) name.init else name // init drop the last $
    }
  }

  sealed trait JobStatus {
    override def toString: String = getClass.getSimpleName.init // init drop the last $
  }

  object DeletionPolicy {
    final case object OnComplete extends DeletionPolicy
    final case object OnSuccess  extends DeletionPolicy
    final case object OnFailure  extends DeletionPolicy
    final case object Never      extends DeletionPolicy
  }

  object JobStatus {
    final case object Running extends JobStatus
    final case object Succeed extends JobStatus
    final case object Failure extends JobStatus
  }
}
