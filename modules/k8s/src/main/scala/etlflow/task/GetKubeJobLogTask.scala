package etlflow.task

import etlflow.k8s.{Jobs, K8S}
import zio.{RIO, ZIO}

/** Gets the logs from the pod where this job was submitted.
  *
  * @param name
  *   Name of this Task
  * @param jobName
  *   Name of the Job
  * @param namespace
  *   Namespace, optional. defaults to 'default'
  * @return
  */
case class GetKubeJobLogTask(name: String, jobName: String, namespace: String = "default") extends EtlTask[Jobs, Unit] {
  override def getTaskProperties: Map[String, String] = Map(
    "name"      -> name,
    "jobName"   -> jobName,
    "namespace" -> namespace
  )

  override protected def process: RIO[Jobs, Unit] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Logging Job $name")
    _ <- K8S
      .getPodLogs(jobName, namespace)
      .tapBoth(
        ex => ZIO.logError(ex.getMessage),
        _ => ZIO.logInfo(s"End of Logs for Job $name") *> ZIO.logInfo("#" * 50)
      )
  } yield ()
}
