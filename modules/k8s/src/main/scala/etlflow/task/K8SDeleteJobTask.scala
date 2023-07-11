package etlflow.task

import etlflow.k8s._
import zio.config._
import ConfigDescriptor._
import zio.{RIO, ZIO}

/** Deletes the Job after specified time
  *
  * @param name
  *   Name of this Task
  * @param jobName
  *   Name of the Job
  * @param namespace
  *   namespace, optional. Defaults to 'default'
  * @param gracePeriodInSeconds
  *   The duration in seconds before the Job should be deleted. Value must be non-negative integer. The value zero indicates
  *   delete immediately. Optional, defaults to 0
  * @param debug
  *   boolean flag which logs more details on some intermediary objects. Optional, defaults to false
  */
case class K8SDeleteJobTask(
    name: String,
    jobName: String,
    namespace: Option[String] = None,
    gracePeriodInSeconds: Option[Int] = None,
    debug: Option[Boolean] = None
) extends EtlTask[K8S, Unit] {

  override def getMetaData: Map[String, String] = Map(
    "name"                   -> name,
    "jobName"                -> jobName,
    "namespace"              -> namespace.getOrElse("default"),
    "debug"                  -> debug.getOrElse(false).toString,
    "deletionGraceInSeconds" -> gracePeriodInSeconds.getOrElse(0).toString
  )

  override protected def process: RIO[K8S, Unit] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Deleting Job $jobName")
    _ <- K8S
      .deleteJob(jobName, namespace.getOrElse("default"), gracePeriodInSeconds.getOrElse(0), debug.getOrElse(false))
      .tapBoth(
        ex => ZIO.logError(ex.getMessage),
        _ => ZIO.logInfo(s"Deleted Job $jobName") *> ZIO.logInfo("#" * 50)
      )
  } yield ()
}

object K8SDeleteJobTask {
  val config: ConfigDescriptor[K8SDeleteJobTask] =
    string("name")
      .zip(string("jobName"))
      .zip(string("namespace").optional)
      .zip(int("gracePeriodInSeconds").optional)
      .zip(boolean("debug").optional)
      .to[K8SDeleteJobTask]
}
