package etlflow.task

import etlflow.k8s._
import io.kubernetes.client.openapi.models.V1Job
import zio.config._
import ConfigDescriptor._
import zio.{RIO, ZIO}

/** Create a Job in a new Container for running an image.
  *
  * @param name
  *   Name of this Task
  * @param jobName
  *   Name of the Job
  * @param image
  *   image descriptor
  * @param container
  *   Name of the Container, Optional, Defaults to Job Name
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
  *   boolean flag which shows the logs from the submitted job.Optional, only used when awaitCompletion is true or deletionPolicy
  *   is not [[etlflow.k8s.DeletionPolicy.Never]]
  * @param pollingFrequencyInMillis
  *   Duration(in milliseconds) to poll for status of the Job. Optional, only used when awaitCompletion is true or deletionPolicy
  *   is not [[etlflow.k8s.DeletionPolicy.Never]]
  * @param deletionPolicy
  *   The deletion policy for this Job. One of: <ul> <li>[[etlflow.k8s.DeletionPolicy.OnComplete]]: Deletes the job when it
  *   completes, regardless for status</li> <li>[[etlflow.k8s.DeletionPolicy.OnSuccess]]: Deletes the Job only if it ran
  *   successfully</li> <li>[[etlflow.k8s.DeletionPolicy.OnFailure]]: Deletes the Job only if it failed</li>
  *   <li>[[etlflow.k8s.DeletionPolicy.Never]]: Does not delete the job</li> </ul> if this is not
  *   [[etlflow.k8s.DeletionPolicy.Never]], then control will wait for job completion, regardless of awaitCompletion
  * @param deletionGraceInSeconds
  *   The duration in seconds before the Job should be deleted. Value must be non-negative integer. The value zero indicates
  *   delete immediately. Optional, defaults to 0
  */
case class K8SJobTask(
    name: String,
    jobName: String,
    image: String,
    container: Option[String] = None,
    imagePullPolicy: Option[String] = None,
    envs: Option[Map[String, String]] = None,
    volumeMounts: Option[Map[String, String]] = None,
    podRestartPolicy: Option[String] = None,
    command: Option[List[String]] = None,
    namespace: Option[String] = None,
    apiVersion: Option[String] = None,
    debug: Option[Boolean] = None,
    awaitCompletion: Option[Boolean] = None,
    showJobLogs: Option[Boolean] = None,
    pollingFrequencyInMillis: Option[Long] = None,
    deletionPolicy: Option[String] = None,
    deletionGraceInSeconds: Option[Int] = None
) extends EtlTask[K8S, V1Job] {

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override val metadata: Map[String, String] = Map(
    "name"                     -> name,
    "jobName"                  -> jobName,
    "container"                -> container.getOrElse(jobName),
    "image"                    -> image,
    "imagePullPolicy"          -> imagePullPolicy.getOrElse("IfNotPresent"),
    "envs"                     -> envs.getOrElse(Map.empty[String, String]).toString,
    "volumeMounts"             -> volumeMounts.getOrElse(Map.empty[String, String]).mkString(", "),
    "podRestartPolicy"         -> podRestartPolicy.getOrElse("OnFailure"),
    "command"                  -> command.getOrElse(Nil).mkString(" "),
    "namespace"                -> namespace.getOrElse("default"),
    "apiVersion"               -> apiVersion.getOrElse("batch/v1"),
    "debug"                    -> debug.getOrElse(false).toString,
    "awaitCompletion"          -> awaitCompletion.getOrElse(false).toString,
    "showJobLogs"              -> showJobLogs.getOrElse(false).toString,
    "pollingFrequencyInMillis" -> pollingFrequencyInMillis.getOrElse(10000L).toString,
    "deletionPolicy"           -> deletionPolicy.getOrElse("Never"),
    "deletionGraceInSeconds"   -> deletionGraceInSeconds.getOrElse(0).toString
  )

  override protected def process: RIO[K8S, V1Job] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Creating K8S Job: $jobName")
    _ <- ZIO.logInfo(s"Image: $image")

    job <- K8S
      .createJob(
        jobName,
        container.getOrElse(jobName),
        image,
        namespace.getOrElse("default"),
        imagePullPolicy.getOrElse("IfNotPresent"),
        envs.getOrElse(Map.empty[String, String]),
        volumeMounts.getOrElse(Map.empty[String, String]),
        command.getOrElse(Nil),
        podRestartPolicy.getOrElse("OnFailure"),
        apiVersion.getOrElse("batch/v1"),
        debug.getOrElse(false),
        awaitCompletion.getOrElse(false),
        showJobLogs.getOrElse(false),
        pollingFrequencyInMillis.getOrElse(10000),
        DeletionPolicy.from(deletionPolicy.getOrElse("Never")),
        deletionGraceInSeconds.getOrElse(0)
      )
      .tapBoth(
        e => ZIO.logError(e.getMessage),
        _ => ZIO.logInfo(s"K8S Job $jobName submitted successfully") *> ZIO.logInfo("#" * 50)
      )
  } yield job
}

object K8SJobTask {
  val config: ConfigDescriptor[K8SJobTask] =
    string("name")
      .zip(string("jobName"))
      .zip(string("image"))
      .zip(string("container").optional)
      .zip(string("imagePullPolicy").optional)
      .zip(map("envs")(string).optional)
      .zip(map("volumeMounts")(string).optional)
      .zip(string("podRestartPolicy").optional)
      .zip(list("command")(string).optional)
      .zip(string("namespace").optional)
      .zip(string("apiVersion").optional)
      .zip(boolean("debug").optional)
      .zip(boolean("awaitCompletion").optional)
      .zip(boolean("showJobLogs").optional)
      .zip(long("pollingFrequencyInMillis").optional)
      .zip(string("deletionPolicy").optional)
      .zip(int("deletionGraceInSeconds").optional)
      .to[K8SJobTask]
}
