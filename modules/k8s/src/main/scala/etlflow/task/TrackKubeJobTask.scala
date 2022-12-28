package etlflow.task

import etlflow.k8s._
import zio.{RIO, ZIO}

case class TrackKubeJobTask(name: String, namespace: String = "default", pollingFrequencyInMillis: Long = 10000)
    extends EtlTask[Jobs, JobStatus] {

  override protected def process: RIO[Jobs, JobStatus] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Polling $name every $pollingFrequencyInMillis milliseconds")
    status <- K8S
      .poll(name, namespace, pollingFrequencyInMillis)
      .tapBoth(
        ex => ZIO.logError(ex.getMessage),
        _ => ZIO.logInfo(s"Done Polling") *> ZIO.logInfo("#" * 50)
      )

  } yield status

  override def getTaskProperties: Map[String, String] = Map(
    "name"                     -> name,
    "namespace"                -> namespace,
    "pollingFrequencyInMillis" -> pollingFrequencyInMillis.toString
  )
}
