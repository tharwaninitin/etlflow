package etlflow.task

import etlflow.k8s._
import io.kubernetes.client.openapi.models.V1Job
import zio.{RIO, ZIO}

case class GetKubeJobTask(name: String, namespace: String = "default", debug: Boolean = false) extends EtlTask[Jobs, V1Job] {

  override protected def process: RIO[Jobs, V1Job] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Getting Job Details for $name")
    job <- K8S
      .getJob(name, namespace, debug)
      .tapBoth(
        ex => ZIO.logError(ex.getMessage),
        _ => ZIO.logInfo(s"Got Job Details for $name") *> ZIO.logInfo("#" * 50)
      )
  } yield job

  override def getTaskProperties: Map[String, String] = Map(
    "name"      -> name,
    "namespace" -> namespace,
    "debug"     -> debug.toString
  )
}
