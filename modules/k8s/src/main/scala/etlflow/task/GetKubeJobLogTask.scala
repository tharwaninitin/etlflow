package etlflow.task

import etlflow.k8s.K8S
import zio.stream.ZPipeline
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
case class GetKubeJobLogTask(name: String, jobName: String, namespace: String = "default") extends EtlTask[K8S, Unit] {

  override protected def process: RIO[K8S, Unit] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- K8S
      .getPodLogs(jobName, namespace)
      .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
      .mapZIO(line => ZIO.logInfo(line))
      .tapError(ex => ZIO.logError(ex.getMessage))
      .runDrain
  } yield ()

  override def getTaskProperties: Map[String, String] = Map(
    "name"      -> name,
    "jobName"   -> jobName,
    "namespace" -> namespace
  )
}
