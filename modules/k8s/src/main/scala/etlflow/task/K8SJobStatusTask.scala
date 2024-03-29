package etlflow.task

import etlflow.k8s.{JobStatus, K8S}
import zio.{RIO, ZIO}

/** Get the status of the task
  *
  * * @param name Name of this Task
  * @param jobName
  *   Name of the Job
  * @param namespace
  *   namespace, optional. Defaults to 'default'
  * @param debug
  *   boolean flag which logs more details on some intermediary objects. Optional, defaults to false
  * @return
  *   JobStatus
  */
case class K8SJobStatusTask(name: String, jobName: String, namespace: String = "default", debug: Boolean = false)
    extends EtlTask[K8S, JobStatus] {

  override protected def process: RIO[K8S, JobStatus] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Getting status for $jobName")
    jobs <- K8S
      .getStatus(jobName, namespace, debug)
      .tapBoth(
        ex => ZIO.logError(ex.getMessage),
        _ => ZIO.logInfo(s"Got status for $namespace") *> ZIO.logInfo("#" * 50)
      )
  } yield jobs

  override val metadata: Map[String, String] = Map(
    "name"      -> name,
    "jobName"   -> jobName,
    "namespace" -> namespace,
    "debug"     -> debug.toString
  )
}
