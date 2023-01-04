package etlflow.task

import etlflow.k8s.{JobStatus, Jobs, K8S}
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
case class GetStatusTask(name: String, jobName: String, namespace: String = "default", debug: Boolean = false)
    extends EtlTask[Jobs, JobStatus] {

  override def getTaskProperties: Map[String, String] = Map(
    "name"      -> name,
    "jobName"   -> jobName,
    "namespace" -> namespace,
    "debug"     -> debug.toString
  )

  override protected def process: RIO[Jobs, JobStatus] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Getting status for $jobName")
    jobs <- K8S
      .getStatus(jobName, namespace, debug)
      .tapBoth(
        ex => ZIO.logError(ex.getMessage),
        _ => ZIO.logInfo(s"Got status for $namespace") *> ZIO.logInfo("#" * 50)
      )
  } yield jobs
}
