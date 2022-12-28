package etlflow.task

import etlflow.k8s._
import io.kubernetes.client.openapi.models.V1Job
import zio.{RIO, ZIO}

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
  *   Volumes to Mount into the Container. Optional. Tuple, with the first element identifying the volume name, and the second the
  *   path to mount inside the container. Optional
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
case class CreateKubeJobTask(
    name: String,
    container: String,
    image: String,
    imagePullPolicy: String = "IfNotPresent",
    envs: Map[String, String] = Map.empty[String, String],
    volumeMounts: List[(String, String)],
    podRestartPolicy: String = "OnFailure",
    command: List[String] = Nil,
    namespace: String = "default",
    apiVersion: String = "batch/v1",
    debug: Boolean = false,
    awaitCompletion: Boolean = false,
    pollingFrequencyInMillis: Long = 10000,
    deletionPolicy: DeletionPolicy = DeletionPolicy.Never,
    deletionGraceInSeconds: Int = 0
) extends EtlTask[Jobs, V1Job] {

  override protected def process: RIO[Jobs, V1Job] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Creating K8S Job: $name")

    job <- K8S
      .createJob(
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
      .tapBoth(
        e => ZIO.logError(e.getMessage),
        _ => ZIO.logInfo(s"K8S Job $name submitted successfully") *> ZIO.logInfo("#" * 50)
      )
  } yield job

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def getTaskProperties: Map[String, String] = Map(
    "name"                     -> name,
    "container"                -> container,
    "image"                    -> image,
    "imagePullPolicy"          -> imagePullPolicy,
    "envs"                     -> envs.toString,
    "volumeMounts"             -> volumeMounts.mkString(", "),
    "podRestartPolicy"         -> podRestartPolicy,
    "command"                  -> command.mkString(" "),
    "namespace"                -> namespace,
    "apiVersion"               -> apiVersion,
    "debug"                    -> debug.toString,
    "awaitCompletion"          -> awaitCompletion.toString,
    "pollingFrequencyInMillis" -> pollingFrequencyInMillis.toString,
    "deletionPolicy"           -> deletionPolicy.toString,
    "deletionGraceInSeconds"   -> deletionGraceInSeconds.toString
  )
}
