package etlflow.task

import etlflow.k8s._
import zio.{RIO, ZIO}

/** Poll the job for completion
  *
  * @param name
  *   Name of this Task
  * @param jobName
  *   Job name
  * @param namespace
  *   Namespace, optional, defaulted to `default`
  * @param pollingFrequencyInMillis
  *   The time in Milliseconds to wait between polls. Optional, defaults to 10000
  * @return
  */
case class TrackKubeJobTask(name: String, jobName: String, namespace: String = "default", pollingFrequencyInMillis: Long = 10000)
    extends EtlTask[Jobs, JobStatus] {

  override def getTaskProperties: Map[String, String] = Map(
    "name"                     -> name,
    "jobName"                  -> jobName,
    "namespace"                -> namespace,
    "pollingFrequencyInMillis" -> pollingFrequencyInMillis.toString
  )

  override protected def process: RIO[Jobs, JobStatus] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Polling $jobName every $pollingFrequencyInMillis milliseconds")
    status <- K8S
      .poll(jobName, namespace, pollingFrequencyInMillis)
      .tapBoth(
        ex => ZIO.logError(ex.getMessage),
        _ => ZIO.logInfo(s"Done Polling") *> ZIO.logInfo("#" * 50)
      )

  } yield status
}
