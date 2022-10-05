package examples

import etlflow._
import etlflow.task._
import zio._

object Job1EtlFlow extends JobApp {

  def executeTask(): Unit = logger.info(s"Hello EtlFlow Task")

  val task1: GenericTask[Unit] = GenericTask(
    name = "Task_1",
    function = executeTask()
  )

  def job(args: Chunk[String]): RIO[audit.AuditEnv, Unit] = task1.execute
}
