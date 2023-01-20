package etlflow

import io.kubernetes.client.openapi.models.V1Job
import zio.stream.ZStream
import zio.{Task, ULayer, ZIO, ZLayer}

package object k8s {

  val noop: ULayer[K8S] = ZLayer.succeed(new K8S {
    override def createJob(
        name: String,
        container: String,
        image: String,
        namespace: String,
        imagePullPolicy: String,
        envs: Map[String, String],
        volumeMounts: Map[String, String],
        command: List[String],
        podRestartPolicy: String,
        apiVersion: String,
        debug: Boolean,
        awaitCompletion: Boolean,
        showJobLogs: Boolean,
        pollingFrequencyInMillis: Long,
        deletionPolicy: DeletionPolicy,
        deletionGraceInSeconds: Int
    ): Task[V1Job] = ZIO.fail(new RuntimeException("Got noop K8S layer, use live layer for actual implementation"))
    override def deleteJob(name: String, namespace: String, gracePeriodInSeconds: Int, debug: Boolean): Task[Unit] =
      ZIO.fail(new RuntimeException("Got noop K8S layer, use live layer for actual implementation"))
    override def getJobs(namespace: String): Task[List[V1Job]] =
      ZIO.fail(new RuntimeException("Got noop K8S layer, use live layer for actual implementation"))
    override def getJob(name: String, namespace: String, debug: Boolean): Task[V1Job] =
      ZIO.fail(new RuntimeException("Got noop K8S layer, use live layer for actual implementation"))
    override def getJobStatus(name: String, namespace: String, debug: Boolean): Task[JobStatus] =
      ZIO.fail(new RuntimeException("Got noop K8S layer, use live layer for actual implementation"))
    override def getPodLogs(jobName: String, namespace: String, chunkSize: Int): ZStream[Any, Throwable, Byte] =
      ZStream.fail(new RuntimeException("Got noop K8S layer, use live layer for actual implementation"))
    override def poll(name: String, namespace: String, pollingFrequencyInMillis: Long): Task[JobStatus] =
      ZIO.fail(new RuntimeException("Got noop K8S layer, use live layer for actual implementation"))
  })

  sealed trait DeletionPolicy {
    override def toString: String = getClass.getSimpleName.init // init drop the last $
  }

  sealed trait JobStatus {
    override def toString: String = getClass.getSimpleName.init // init drop the last $
  }

  object DeletionPolicy {

    def from(deletionPolicy: String): DeletionPolicy = deletionPolicy match {
      case "OnComplete" => OnComplete
      case "OnSuccess"  => OnSuccess
      case "OnFailure"  => OnFailure
      case "Never"      => Never
    }
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
