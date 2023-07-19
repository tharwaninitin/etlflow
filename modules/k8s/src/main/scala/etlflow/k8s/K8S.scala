package etlflow.k8s

import io.kubernetes.client.openapi.ApiClient
import io.kubernetes.client.openapi.apis.{BatchV1Api, CoreV1Api}
import io.kubernetes.client.openapi.models.V1Job
import zio.stream.ZStream
import zio.{RIO, Scope, Task, TaskLayer, ZIO, ZLayer}

/** API for managing resources on Kubernetes Cluster
  */
trait K8S {

  /** Create a Job in a new Container for running an image.
    *
    * @param name
    *   Name of the Job
    * @param container
    *   Name of the Container
    * @param image
    *   image descriptor
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @param envs
    *   Environment Variables to set for the container. Optional
    * @param volumeMounts
    *   Volumes to Mount into the Container. Optional. Map, with the first element identifying the path to mount inside the
    *   container, and the second the volume name. Optional
    * @param command
    *   Entrypoint array. Not executed within a shell. The container image's ENTRYPOINT is used if this is not provided. Optional
    * @param podRestartPolicy
    *   Restart policy for the container. One of Always, OnFailure, Never. Default to Never. More info:
    *   https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-policy
    * @param apiVersion
    *   API Version, Optional, defaults to batch/v1
    * @param debug
    *   boolean flag which logs more details on some intermediary objects. Optional, defaults to false
    * @param awaitCompletion
    *   boolean flag which indicates whether control should await for the job's completion before returning
    * @param showJobLogs
    *   boolean flag which shows the logs from the submitted job. Optional, only used when awaitCompletion is true or
    *   deletionPolicy is not [[DeletionPolicy.Never]]
    * @param pollingFrequencyInMillis
    *   Duration(in milliseconds) to poll for status of the Job. Optional, only used when awaitCompletion is true or
    *   deletionPolicy is not [[DeletionPolicy.Never]]
    * @param deletionPolicy
    *   The deletion policy for this Job. One of: <ul> <li>[[DeletionPolicy.OnComplete]]: Deletes the job when it completes,
    *   regardless for status</li> <li>[[DeletionPolicy.OnSuccess]]: Deletes the Job only if it ran successfully</li>
    *   <li>[[DeletionPolicy.OnFailure]]: Deletes the Job only if it failed</li> <li>[[DeletionPolicy.Never]]: Does not delete the
    *   job</li> </ul> if this is not [[DeletionPolicy.Never]], then control will wait for job completion, regardless of
    *   awaitCompletion
    * @param deletionGraceInSeconds
    *   The duration in seconds before the Job should be deleted. Value must be non-negative integer. The value zero indicates
    *   delete immediately. Optional, defaults to 0
    */
  // noinspection ScalaStyle
  def createJob(
      name: String,
      container: String,
      image: String,
      namespace: String = "default",
      imagePullPolicy: String = "IfNotPresent",
      envs: Map[String, String] = Map.empty[String, String],
      volumeMounts: Map[String, String] = Map.empty[String, String],
      command: List[String] = Nil,
      podRestartPolicy: String = "Never",
      apiVersion: String = "batch/v1",
      debug: Boolean = false,
      awaitCompletion: Boolean = false,
      showJobLogs: Boolean = false,
      pollingFrequencyInMillis: Long = 10000,
      deletionPolicy: DeletionPolicy = DeletionPolicy.Never,
      deletionGraceInSeconds: Int = 0
  ): Task[V1Job]

  def executeCoreApiTask[T](f: CoreV1Api => T): Task[T]

  def executeBatchApiTask[T](f: BatchV1Api => T): Task[T]

  def getApiClient[T]: Task[ApiClient]

  /** Deletes the Job after specified time
    *
    * @param name
    *   Name of the Job
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @param gracePeriodInSeconds
    *   The duration in seconds before the Job should be deleted. Value must be non-negative integer. The value zero indicates
    *   delete immediately. Optional, defaults to 0
    * @param debug
    *   boolean flag which logs more details on some intermediary objects. Optional, defaults to false
    */
  def deleteJob(name: String, namespace: String = "default", gracePeriodInSeconds: Int = 0, debug: Boolean = false): Task[Unit]

  /** Returns a list of all the job running in the provided namespace
    *
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @return
    *   A list of Job names
    */
  def getJobs(namespace: String = "default"): Task[List[V1Job]]

  /** Returns the job running in the provided namespace
    *
    * @param name
    *   Name of the job
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @param debug
    *   boolean flag which logs more details on some intermediary objects. Optional, defaults to false
    * @return
    *   A Job, as an instance of T
    */
  def getJob(name: String, namespace: String = "default", debug: Boolean = false): Task[V1Job]

  /** @param name
    *   Name of the Job
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @param debug
    *   boolean flag which logs more details on some intermediary objects. Optional, defaults to false
    * @return
    */
  def getJobStatus(name: String, namespace: String = "default", debug: Boolean = false): Task[JobStatus]

  /** Gets the logs from the pod for the job.
    *
    * @param jobName
    *   Name of the Job
    * @param namespace
    *   Namespace, optional. defaults to 'default'
    * @param chunkSize
    *   Chunk size for fetch logs(bytes) from K8S
    * @return
    */
  def getPodLogs(jobName: String, namespace: String = "default", chunkSize: Int = 4096): ZStream[Any, Throwable, Byte]

  /** Poll the job for completion
    * @param name
    *   Job name
    * @param namespace
    *   Namespace, optional, defaulted to `default`
    * @param pollingFrequencyInMillis
    *   The time in Milliseconds to wait between polls. Optional, defaults to 10000
    * @return
    */
  def poll(name: String, namespace: String = "default", pollingFrequencyInMillis: Long = 10000): Task[JobStatus]
}

object K8S {

  // noinspection ScalaStyle
  def createJob(
      name: String,
      container: String,
      image: String,
      namespace: String = "default",
      imagePullPolicy: String = "IfNotPresent",
      envs: Map[String, String] = Map.empty[String, String],
      volumeMounts: Map[String, String] = Map.empty[String, String],
      command: List[String] = Nil,
      podRestartPolicy: String = "Never",
      apiVersion: String = "batch/v1",
      debug: Boolean = false,
      awaitCompletion: Boolean = true,
      showJobLogs: Boolean = false,
      pollingFrequencyInMillis: Long = 10000,
      deletionPolicy: DeletionPolicy = DeletionPolicy.Never,
      deletionGraceInSeconds: Int = 0
  ): RIO[K8S, V1Job] =
    ZIO.environmentWithZIO[K8S](
      _.get.createJob(
        name,
        container,
        image,
        namespace,
        imagePullPolicy,
        envs,
        volumeMounts,
        command,
        podRestartPolicy,
        apiVersion,
        debug,
        awaitCompletion,
        showJobLogs,
        pollingFrequencyInMillis,
        deletionPolicy,
        deletionGraceInSeconds
      )
    )

  def executeCoreApiTask[T](f: CoreV1Api => T): RIO[K8S, T] = ZIO.serviceWithZIO[K8S](_.executeCoreApiTask(f))

  def executeBatchApiTask[T](f: BatchV1Api => T): RIO[K8S, T] = ZIO.serviceWithZIO[K8S](_.executeBatchApiTask(f))

  def getApiClient[T]: RIO[K8S, ApiClient] = ZIO.serviceWithZIO[K8S](_.getApiClient)

  def deleteJob(
      name: String,
      namespace: String = "default",
      gracePeriodInSeconds: Int = 0,
      debug: Boolean = false
  ): RIO[K8S, Unit] =
    ZIO.environmentWithZIO[K8S](_.get.deleteJob(name, namespace, gracePeriodInSeconds, debug))

  def getJobs(namespace: String = "default"): RIO[K8S, List[V1Job]] =
    ZIO.environmentWithZIO[K8S](_.get.getJobs(namespace))

  def getJob(
      name: String,
      namespace: String = "default",
      debug: Boolean = false
  ): RIO[K8S, V1Job] =
    ZIO.environmentWithZIO[K8S](_.get.getJob(name, namespace, debug))

  def getStatus(
      name: String,
      namespace: String = "default",
      debug: Boolean = false
  ): RIO[K8S, JobStatus] =
    ZIO.environmentWithZIO[K8S](_.get.getJobStatus(name, namespace, debug))

  def poll(name: String, namespace: String = "default", pollingFrequencyInMillis: Long = 10000): RIO[K8S, JobStatus] =
    ZIO.environmentWithZIO[K8S](_.get.poll(name, namespace, pollingFrequencyInMillis))

  def getPodLogs(jobName: String, namespace: String, chunkSize: Int = 4096): ZStream[K8S, Throwable, Byte] =
    ZStream.environmentWithStream[K8S](_.get.getPodLogs(jobName, namespace, chunkSize))

  /** Method: live - Provides layer to execute K8S Job APIs
    * @param httpConnectionTimeout
    *   Http request connection timeout in MILLISECONDS, A value of 0 means no timeout
    * @param apiClient
    *   If provided it will be used or ApiClient will be created
    * @return
    *   TaskLayer[K8S]
    */
  def live(httpConnectionTimeout: Int = 100000, apiClient: Option[ApiClient] = None): TaskLayer[K8S] = ZLayer.scoped {
    val client: ZIO[Scope, Throwable, ApiClient] = apiClient match {
      case Some(apiClient) => ZIO.succeed(apiClient)
      case None            => K8SClient.createApiClient(httpConnectionTimeout)
    }
    client
      .map(K8SClient.setDefaultApiClient)
      .map(client => K8SImpl(K8SClient.createCoreClient(client), K8SClient.createBatchClient(client), client))
  }
}
