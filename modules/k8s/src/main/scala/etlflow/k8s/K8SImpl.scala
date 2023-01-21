package etlflow.k8s

import etlflow.k8s.DeletionPolicy._
import etlflow.k8s.JobStatus._
import etlflow.log.ApplicationLogger
import etlflow.model.EtlFlowException.RetryException
import etlflow.utils.RetrySchedule
import io.kubernetes.client.PodLogs
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.apis.{BatchV1Api, CoreV1Api}
import io.kubernetes.client.openapi.models._
import zio.stream.{ZPipeline, ZStream}
import zio.{Task, ZIO}
import scala.concurrent.duration.DurationLong
import scala.jdk.CollectionConverters._

@SuppressWarnings(Array("org.wartremover.warts.AutoUnboxing", "org.wartremover.warts.Null", "org.wartremover.warts.ToString"))
case class K8SImpl(batch: BatchV1Api, core: CoreV1Api) extends K8S with ApplicationLogger {
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
  ): Task[V1Job] = for {

    v1Job <- ZIO.attempt(
      createV1JobInstance(name, container, image, imagePullPolicy, envs, volumeMounts, command, podRestartPolicy, apiVersion)
    )
    _ <- ZIO.logInfo(v1Job.toString).when(debug)
    _ <- logJobs(namespace).when(debug)
    job <- ZIO
      .attempt(batch.createNamespacedJob(namespace, v1Job, debug.toString, null, null, null))
      .tapBoth(
        {
          case exception: ApiException => ZIO.logError(exception.getResponseBody) *> ZIO.fail(exception)
          case exception               => ZIO.logError(exception.getMessage) *> ZIO.fail(exception)
        },
        job =>
          for {
            _ <- ZIO.logInfo(s"Job $name deployed successfully")
            _ <- logJobs(namespace).when(debug)
            _ <- poll(name, namespace, debug, pollingFrequencyInMillis, showJobLogs, deletionPolicy, deletionGraceInSeconds)
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
      showJobLogs: Boolean,
      deletionPolicy: DeletionPolicy,
      deletionGraceInSeconds: Int
  ): Task[Unit] =
    for {
      result <- poll(name, namespace, pollingFrequencyInMillis)
      _      <- ZIO.logInfo(s"Job $name finished with result: $result")
      _ <- getPodLogs(name, namespace)
        .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
        .mapZIO(line => ZIO.logInfo(line))
        .tapError(ex => ZIO.logError(ex.getMessage))
        .runDrain
        .when(showJobLogs)
      _ <- (deletionPolicy, result) match {
        case (OnComplete, _) | (OnSuccess, Succeed) | (OnFailure, JobStatus.Failure) =>
          deleteJob(name, namespace, deletionGraceInSeconds, debug)
        case _ => ZIO.logInfo(s"Job $name will remain until manually deleted($deletionPolicy did not comply with $result)")
      }
    } yield ()

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
      val _ = batch.deleteNamespacedJob(
        name,
        namespace,
        debug.toString,
        null,
        gracePeriodInSeconds,
        null,
        "Foreground",
        new V1DeleteOptions()
      )
    }
    .tapError {
      case exception: ApiException => ZIO.logError(exception.getResponseBody) *> ZIO.fail(exception)
      case exception               => ZIO.logError(exception.getMessage) *> ZIO.fail(exception)
    }

  /** Poll the job for completion
    * @param name
    *   Job name
    * @param namespace
    *   Namespace, optional, defaulted to `default`
    * @param pollingFrequencyInMillis
    *   The time in Milliseconds to wait between polls. Optional, defaults to 10000
    * @return
    */
  override def poll(name: String, namespace: String, pollingFrequencyInMillis: Long): Task[JobStatus] = (
    for {
      status <- getJobStatus(name, namespace, debug = true)
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
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  override def getJobStatus(name: String, namespace: String, debug: Boolean): Task[JobStatus] = for {
    jobStatus <- getJob(name, namespace, debug).map(_.getStatus)
    pod       <- getJobPod(name, namespace)
    podStatus <- ZIO.attempt(core.readNamespacedPodStatus(pod.getMetadata.getName, namespace, "false").getStatus)
    status = s"Pod ${pod.getMetadata.getName}: ${podStatus.getPhase} [" +:
      podStatus.getConditions.asScala.map { condition =>
        val explanations =
          List(Option(condition.getReason).fold("")("Reason: " + _), Option(condition.getMessage).fold("")("Message: " + _))
            .filter(_.nonEmpty)

        if (explanations.isEmpty)
          s"\t${condition.getType}: ${condition.getStatus}"
        else
          s"\t${condition.getType}: ${condition.getStatus} ${explanations.mkString("(", ", ", ")")}"
      } :+ "]"
    _ <- ZIO.foreachDiscard(status.toList)(x => ZIO.logInfo(x))
  } yield
    if (jobStatus.getActive != null) JobStatus.Running
    else if (jobStatus.getFailed == null) JobStatus.Succeed
    else JobStatus.Failure

  /** Returns the job running in the provided namespace
    *
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @return
    *   V1Job
    */
  override def getJob(name: String, namespace: String, debug: Boolean): Task[V1Job] = ZIO
    .attempt {
      batch.readNamespacedJobStatus(name, namespace, debug.toString)
    }
    .tapError {
      case exception: ApiException => ZIO.logError(exception.getResponseBody) *> ZIO.fail(exception)
      case exception               => ZIO.logError(exception.getMessage) *> ZIO.fail(exception)
    }

  /** Gets the logs from the pod where this job was submitted.
    *
    * @param jobName
    *   Name of the Job
    * @param namespace
    *   Namespace
    * @param chunkSize
    *   Chunk size for fetch logs(bytes) from K8S
    */
  override def getPodLogs(jobName: String, namespace: String, chunkSize: Int): ZStream[Any, Throwable, Byte] = {
    val byteStream = for {
      // noinspection ScalaStyle
      pod <- ZStream.fromZIO(getJobPod(jobName, namespace))
      logs   = new PodLogs()
      stream = logs.streamNamespacedPodLog(pod)
      _      = logger.info(s"Streaming logs from pod ${pod.getMetadata.getName} for job $jobName")
      logByte <- ZStream.fromInputStream(stream, chunkSize)
    } yield logByte

    byteStream
      .tapError {
        case ex: ApiException => ZIO.logError(ex.getResponseBody)
        case ex               => ZIO.logError(ex.getMessage)
      }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Throw", "org.wartremover.warts.MutableDataStructures"))
  private def getJobPod(jobName: String, namespace: String): Task[V1Pod] = ZIO.attempt {
    // noinspection ScalaStyle
    val pods = core
      .listNamespacedPod(namespace, null, null, null, null, null, null, null, null, null, null)
      .getItems
      .asScala
    pods.find(p => p.getMetadata.getName.startsWith(jobName)) match {
      case Some(pod) => pod
      case None =>
        throw new Exception(
          s"Cannot find $jobName in Pods ${pods.map(_.getMetadata.getName).mkString(", ")} in $namespace namespace"
        )
    }
  }

  private def createV1JobInstance(
      name: String,
      container: String,
      image: String,
      imagePullPolicy: String,
      envs: Map[String, String],
      volumeMounts: Map[String, String],
      command: List[String],
      podRestartPolicy: String,
      apiVersion: String
  ): V1Job = {
    val meta          = new V1ObjectMeta().name(name)
    val containerItem = new V1Container().name(container).image(image).imagePullPolicy(imagePullPolicy)
    command.foreach(containerItem.addCommandItem)
    envs.foreach { case (key, value) => containerItem.addEnvItem(new V1EnvVar().name(key).value(value)) }
    volumeMounts.zipWithIndex.foreach { case ((path, _), i) =>
      containerItem.addVolumeMountsItem(
        new V1VolumeMount()
          .name(s"$secretKey-$i")
          .mountPath(path)
          .readOnly(true)
      )
    }

    val spec = new V1PodSpec()
      .restartPolicy(podRestartPolicy)
      .addContainersItem(containerItem)
    volumeMounts.zipWithIndex.foreach { case ((_, secretName), i) =>
      spec.addVolumesItem(
        new V1Volume()
          .name(s"$secretKey-$i")
          .secret(
            new V1SecretVolumeSource()
              .secretName(secretName)
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
    _    <- ZIO.foreachDiscard(jobs.map(_.getMetadata.getName))(x => ZIO.logInfo(x))
  } yield ()

  /** Returns a list of all the job running in the provided namespace
    *
    * @param namespace
    *   namespace, optional. Defaults to 'default'
    * @return
    *   A list of Jobs, as instances of V1Job
    */
  override def getJobs(namespace: String): Task[List[V1Job]] = ZIO.attempt {
    // noinspection ScalaStyle
    batch
      .listNamespacedJob(namespace, null, null, null, null, null, null, null, null, null, null)
      .getItems
      .asScala
      .toList
  }
}
