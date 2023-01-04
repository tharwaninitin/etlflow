package etlflow.task

import etlflow.k8s._
import io.kubernetes.client.openapi.models.V1Job
import zio.{RIO, ZIO}

/** Returns the job running in the provided namespace
  *
  * @param name
  *   Name of this Task
  * @param jobName
  *   Name of the job
  * @param namespace
  *   namespace, optional. Defaults to 'default'
  * @param debug
  *   boolean flag which logs more details on some intermediary objects. Optional, defaults to false
  * @return
  *   A Job, as an instance of V1Job
  */
case class GetKubeJobTask(name: String, jobName: String, namespace: String = "default", debug: Boolean = false)
    extends EtlTask[K8S, V1Job] {

  override protected def process: RIO[K8S, V1Job] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Getting Job Details for $jobName")
    job <- K8S
      .getJob(jobName, namespace, debug)
      .tapBoth(
        ex => ZIO.logError(ex.getMessage),
        _ => ZIO.logInfo(s"Got Job Details for $jobName") *> ZIO.logInfo("#" * 50)
      )
  } yield job

  override def getTaskProperties: Map[String, String] = Map(
    "name"      -> name,
    "jobName"   -> jobName,
    "namespace" -> namespace,
    "debug"     -> debug.toString
  )
}
