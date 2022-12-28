package etlflow.k8s

import io.kubernetes.client.openapi.models.V1Job
import zio.{RIO, Task, TaskLayer, ZIO, ZLayer}

/** API for managing resources on Kubernetes Cluster
  *
  * @tparam T
  *   The Job Datatype
  */
trait K8S[T] {

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
    *   Volumes to Mount into the Container. Optional. Tuple, with the first element identifying the volume name, and the second
    *   the path to mount inside the container. Optional
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
      volumeMounts: List[(String, String)] = Nil,
      command: List[String] = Nil,
      podRestartPolicy: String = "Never",
      apiVersion: String = "batch/v1",
      debug: Boolean = false,
      awaitCompletion: Boolean = false,
      pollingFrequencyInMillis: Long = 10000,
      deletionPolicy: DeletionPolicy = DeletionPolicy.Never,
      deletionGraceInSeconds: Int = 0
  ): Task[T]

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
  def deleteJob(
      name: String,
      namespace: String = "default",
      gracePeriodInSeconds: Int = 0,
      debug: Boolean = false
  ): Task[Unit]

  /** Returns a list of all the job running in the provided namespace
    *
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @return
    *   A list of Job names
    */
  def getJobs(namespace: String = "default"): Task[List[String]]

  /** Returns the job running in the provided namespace
    *
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @return
    *   A Job, as an instance of T
    */
  def getJob(
      name: String,
      namespace: String = "default",
      debug: Boolean = false
  ): Task[T]

  /** @param name
    *   Name of the Job
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @param debug
    *   boolean flag which logs more details on some intermediary objects. Optional, defaults to false
    * @return
    */
  def getStatus(
      name: String,
      namespace: String = "default",
      debug: Boolean = false
  ): Task[JobStatus]

  def poll(name: String, namespace: String, pollingFrequencyInMillis: Long): Task[JobStatus]
}

object K8S {

  /** Method: batchClient - Provides layer to execute K8S Job APIs
    * @param httpConnectionTimeout
    *   Http request connection timeout in MILLISECONDS, A value of 0 means no timeout
    * @return
    *   TaskLayer[Jobs]
    */
  def batchClient(httpConnectionTimeout: Int = 100000): TaskLayer[Jobs] = ZLayer.fromZIO {
    ZIO
      .attempt(K8SClient.batchClient(httpConnectionTimeout))
      .map(bc => K8SJobImpl(bc))
  }

  // noinspection ScalaStyle
  def createJob(
      name: String,
      container: String,
      image: String,
      namespace: String = "default",
      imagePullPolicy: String = "IfNotPresent",
      envs: Map[String, String] = Map.empty[String, String],
      volumeMounts: List[(String, String)] = Nil,
      command: List[String] = Nil,
      podRestartPolicy: String = "Never",
      apiVersion: String = "batch/v1",
      debug: Boolean = false,
      awaitCompletion: Boolean = true,
      pollingFrequencyInMillis: Long = 10000,
      deletionPolicy: DeletionPolicy = DeletionPolicy.Never,
      deletionGraceInSeconds: Int = 0
  ): RIO[Jobs, V1Job] =
    ZIO.environmentWithZIO[Jobs](
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
        pollingFrequencyInMillis,
        deletionPolicy,
        deletionGraceInSeconds
      )
    )

  def deleteJob(
      name: String,
      namespace: String = "default",
      gracePeriodInSeconds: Int = 0,
      debug: Boolean = false
  ): RIO[Jobs, Unit] =
    ZIO.environmentWithZIO[Jobs](_.get.deleteJob(name, namespace, gracePeriodInSeconds, debug))

  def getJobs(namespace: String = "default"): RIO[Jobs, List[String]] =
    ZIO.environmentWithZIO[Jobs](_.get.getJobs(namespace))

  def getJob(
      name: String,
      namespace: String = "default",
      debug: Boolean = false
  ): RIO[Jobs, V1Job] =
    ZIO.environmentWithZIO[Jobs](_.get.getJob(name, namespace, debug))

  def getStatus(
      name: String,
      namespace: String = "default",
      debug: Boolean = false
  ): RIO[Jobs, JobStatus] =
    ZIO.environmentWithZIO[Jobs](_.get.getStatus(name, namespace, debug))

  def poll(name: String, namespace: String, pollingFrequencyInMillis: Long): RIO[Jobs, JobStatus] =
    ZIO.environmentWithZIO[Jobs](_.get.poll(name, namespace, pollingFrequencyInMillis))
}
