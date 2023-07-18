package examples

import etlflow.audit.Audit
import etlflow.task.GenericTask
import zio._

object Job1 extends ZIOAppDefault {

  def executeTask(): Task[Unit] = ZIO.logInfo(s"Hello EtlFlow Task")

  val genericTask1: GenericTask[Any, Unit] = GenericTask(
    name = "Generic Task",
    task = executeTask()
  )

  val task1: RIO[Audit, Unit] = genericTask1.toZIO

  override def run: Task[Unit] = task1.provide(etlflow.audit.noop)
}
