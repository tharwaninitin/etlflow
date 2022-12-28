package etlflow.k8s

import etlflow.k8s.DeletionPolicy._
import etlflow.k8s.JobStatus._
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.openapi.models._
import zio.{Task, ZIO}

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationLong

@SuppressWarnings(
  Array(
    "org.wartremover.warts.AutoUnboxing",
    "org.wartremover.warts.Null"
  )
)
case class K8SJobImpl(api: BatchV1Api) extends K8S[V1Job] {
  private val secretKey = "secret"

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
    *   the path to mount inside the container. It is recommended to use [[scala.Predef.ArrowAssoc.->]] for readability. Optional
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
  override def createJob(
      name: String,
      container: String,
      image: String,
      namespace: String,
      imagePullPolicy: String,
      envs: Map[String, String],
      volumeMounts: List[(String, String)],
      command: List[String],
      podRestartPolicy: String,
      apiVersion: String,
      debug: Boolean,
      awaitCompletion: Boolean,
      pollingFrequencyInMillis: Long,
      deletionPolicy: DeletionPolicy,
      deletionGraceInSeconds: Int
  ): Task[V1Job] = for {

    v1Job <- ZIO.attempt(
      createV1JobInstance(name, container, image, imagePullPolicy, envs, volumeMounts, command, podRestartPolicy, apiVersion)
    )
    _ <- ZIO.logInfo(v1Job.toString).when(debug)
    _ <- logJobs(namespace).when(debug)
    job <- ZIO
      .attempt(api.createNamespacedJob(namespace, v1Job, debug.toString, null, null, null))
      .tapBoth(
        {
          case exception: ApiException => ZIO.logError(exception.getResponseBody) *> ZIO.fail(exception)
          case exception               => ZIO.logError(exception.getMessage) *> ZIO.fail(exception)
        },
        job =>
          for {
            _ <- ZIO.logInfo(s"Job $name deployed successfully")
            _ <- logJobs(namespace).when(debug)
            _ <- poll(name, namespace, debug, pollingFrequencyInMillis, deletionPolicy, deletionGraceInSeconds)
              .when(awaitCompletion || deletionPolicy != Never)
          } yield job
      )
    _ <- logJobs(namespace).when(debug)

  } yield job

  private def poll(
      name: String,
      namespace: String,
      debug: Boolean,
      pollingFrequencyInMillis: Long,
      deletionPolicy: DeletionPolicy,
      deletionGraceInSeconds: Int
  ): Task[Unit] =
    for {
      result <- poll(name, namespace, pollingFrequencyInMillis)
      _      <- ZIO.logInfo(s"Job $name finished with result: $result")
      _ <- (deletionPolicy, result) match {
        case (OnComplete, _) | (OnSuccess, Succeed) | (OnFailure, Failure) =>
          deleteJob(name, namespace, deletionGraceInSeconds, debug)
        case _ => ZIO.logInfo(s"Job $name will remain until manually deleted($deletionPolicy did not comply with $result)")
      }
    } yield ()

  override def poll(name: String, namespace: String, pollingFrequencyInMillis: Long): Task[JobStatus] = (
    for {
      status <- getStatus(name, namespace, debug = true)
      _      <- ZIO.when(status == JobStatus.Running)(ZIO.fail(RetryException(s"$name is running. Polling again.")))
      _      <- ZIO.logInfo(s"Job $name completed with status $status")
    } yield status
  ).retry(RetrySchedule.forever(pollingFrequencyInMillis.milliseconds))

  /** @param name
    *   Name of the Job
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @param debug
    *   boolean flag which logs more details on some intermediary objects. Optional, defaults to false
    * @return
    */
  override def getStatus(name: String, namespace: String, debug: Boolean): Task[JobStatus] = for {
    _         <- ZIO.logInfo(s"Getting $name's Status'").when(debug)
    jobStatus <- ZIO.attempt(api.readNamespacedJobStatus(name, namespace, debug.toString).getStatus)
    _ <- ZIO
      .logInfo(
        s"Active: ${sanitize(jobStatus.getActive)}, " +
          s"FAILED: ${sanitize(jobStatus.getFailed)}, " +
          s"PASSED: ${sanitize(jobStatus.getSucceeded)}"
      )
      .when(debug)
  } yield
    if (jobStatus.getActive != null) JobStatus.Running
    else if (jobStatus.getFailed == null) JobStatus.Succeed
    else JobStatus.Failure

  private def sanitize(option: Int) = Option(option).getOrElse(0)

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
  override def deleteJob(name: String, namespace: String, gracePeriodInSeconds: Int, debug: Boolean): Task[Unit] = ZIO
    .attempt {
      // noinspection ScalaStyle
      val _ = api.deleteNamespacedJob(
        name,
        namespace,
        debug.toString,
        null,
        gracePeriodInSeconds,
        null,
        null,
        new V1DeleteOptions()
      )
    }
    .tapError {
      case exception: ApiException => ZIO.logError(exception.getResponseBody) *> ZIO.fail(exception)
      case exception               => ZIO.logError(exception.getMessage) *> ZIO.fail(exception)
    }

  private def createV1JobInstance(
      name: String,
      container: String,
      image: String,
      imagePullPolicy: String,
      envs: Map[String, String],
      volumeMounts: List[(String, String)],
      command: List[String],
      podRestartPolicy: String,
      apiVersion: String
  ): V1Job = {
    val meta          = new V1ObjectMeta().name(name)
    val containerItem = new V1Container().name(container).image(image).imagePullPolicy(imagePullPolicy)
    command.foreach(containerItem.addCommandItem)
    envs.foreach { case (key, value) => containerItem.addEnvItem(new V1EnvVar().name(key).value(value)) }
    volumeMounts.zipWithIndex.foreach { case ((_, destination), i) =>
      containerItem.addVolumeMountsItem(
        new V1VolumeMount()
          .name(s"$secretKey-$i")
          .mountPath(destination)
          .readOnly(true)
      )
    }

    val spec = new V1PodSpec()
      .restartPolicy(podRestartPolicy)
      .addContainersItem(containerItem)
    volumeMounts.zipWithIndex.foreach { case ((source, _), i) =>
      spec.addVolumesItem(
        new V1Volume()
          .name(s"$secretKey-$i")
          .secret(
            new V1SecretVolumeSource()
              .secretName(source)
              .optional(false)
          )
      )
    }

    new V1Job()
      .apiVersion(apiVersion)
      .kind("Job")
      .metadata(meta)
      .spec(
        new V1JobSpec()
          .template(
            new V1PodTemplateSpec()
              .metadata(meta)
              .spec(spec)
          )
      )
  }

  private def logJobs(namespace: String): Task[Unit] = for {
    jobs <- getJobs(namespace)
    _    <- ZIO.foreach(jobs)(x => ZIO.logInfo(x))
  } yield ()

  /** Returns a list of all the job running in the provided namespace
    *
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @return
    *   A list of Jobs, as instances of [[V1Job]]
    */
  override def getJobs(namespace: String): Task[List[String]] = ZIO.attempt {
    // noinspection ScalaStyle
    api
      .listNamespacedJob(namespace, null, null, null, null, null, null, null, null, null, null)
      .getItems
      .asScala
      .map(_.getMetadata.getName)
      .toList
  }

  /** Returns the job running in the provided namespace
    *
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @return
    *   A [[V1Job]]
    */
  override def getJob(name: String, namespace: String, debug: Boolean): Task[V1Job] = ZIO
    .attempt {
      api.readNamespacedJobStatus(name, namespace, debug.toString)
    }
    .tapError {
      case exception: ApiException => ZIO.logError(exception.getResponseBody) *> ZIO.fail(exception)
      case exception               => ZIO.logError(exception.getMessage) *> ZIO.fail(exception)
    }
}
