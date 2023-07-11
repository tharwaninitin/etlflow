package examples

import etlflow._
import etlflow.task._
import zio._

object Job1EtlFlow extends JobApp {

  def executeTask(): Task[Unit] = ZIO.logInfo(s"Hello EtlFlow Task")

  val task1: GenericTask[Any, Unit] = GenericTask(
    name = "Task_1",
    task = executeTask()
  )

  def job(args: Chunk[String]): RIO[audit.Audit, Unit] = task1.toZIO
}
