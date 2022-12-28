package etlflow.task

import etlflow.k8s.{Jobs, K8S}
import zio.{RIO, ZIO}

/** Returns a list of all the job running in the provided namespace
  *
  * @param namespace
  *   namespace, optional. Defaults to 'default'
  * @return
  *   A list of Job names
  */
case class GetKubeJobsTask(name: String, namespace: String = "default") extends EtlTask[Jobs, List[String]] {

  override def process: RIO[Jobs, List[String]] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Getting jobs in $namespace")
    jobs <- K8S
      .getJobs(namespace)
      .tapBoth(
        ex => ZIO.logError(ex.getMessage),
        _ => ZIO.logInfo(s"Got jobs in $namespace") *> ZIO.logInfo("#" * 50)
      )

  } yield jobs

  override def getTaskProperties: Map[String, String] = Map("namespace" -> namespace)
}
