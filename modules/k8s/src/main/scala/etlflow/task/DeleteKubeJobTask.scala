package etlflow.task

import etlflow.k8s._
import zio.{RIO, ZIO}

/** Deletes the Job after specified time
  *
  * @param name
  *   Name of the Job
  * @param namespace
  *   namespace, optional. Defaults to 'default'
  * @param gracePeriodInSeconds
  *   The duration in seconds before the Job should be deleted. Value must be non-negative integer. The value zero indicates
  *   delete immediately. Optional, defaults to 0
  * @param debug
  *   boolean flag which logs more details on some intermediary objects. Optional, defaults to false
  */
case class DeleteKubeJobTask(name: String, namespace: String = "default", gracePeriodInSeconds: Int = 0, debug: Boolean = false)
    extends EtlTask[Jobs, Unit] {

  override protected def process: RIO[Jobs, Unit] = for {
    _ <- ZIO.logInfo("#" * 50)
    _ <- ZIO.logInfo(s"Deleting Job $name")
    _ <- K8S
      .deleteJob(name, namespace, gracePeriodInSeconds, debug)
      .tapBoth(
        ex => ZIO.logError(ex.getMessage),
        _ => ZIO.logInfo(s"Deleted Job $name") *> ZIO.logInfo("#" * 50)
      )
  } yield ()

  override def getTaskProperties: Map[String, String] = Map(
    "name"                   -> name,
    "namespace"              -> namespace,
    "debug"                  -> debug.toString,
    "deletionGraceInSeconds" -> gracePeriodInSeconds.toString
  )
}
