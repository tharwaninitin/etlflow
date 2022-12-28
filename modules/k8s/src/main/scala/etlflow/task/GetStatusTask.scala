package etlflow.task

import etlflow.k8s.{JobStatus, Jobs, K8S}
import zio.{RIO, ZIO}

case class GetStatusTask(name: String, namespace: String = "default", debug: Boolean = false) extends EtlTask[Jobs, JobStatus] {

  override protected def process: RIO[Jobs, JobStatus] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Getting status for $name")
    jobs <- K8S
      .getStatus(name, namespace, debug)
      .tapBoth(
        ex => ZIO.logError(ex.getMessage),
        _ => ZIO.logInfo(s"Got status for $namespace") *> ZIO.logInfo("#" * 50)
      )
  } yield jobs

  override def getTaskProperties: Map[String, String] = Map(
    "name"      -> name,
    "namespace" -> namespace,
    "debug"     -> debug.toString
  )
}
