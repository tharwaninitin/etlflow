package examples

import etlflow.log.ApplicationLogger
import etlflow.task.GenericTask
import zio._

object Job1Logger extends zio.ZIOAppDefault with ApplicationLogger {

  override val bootstrap = zioSlf4jLogger

  def executeTask(): Task[Unit] = ZIO.logInfo(s"Hello EtlFlow Task")

  private val task1 = GenericTask(
    name = "Task_1",
    task = executeTask()
  )

  override def run: Task[Unit] = task1.toZIO.provide(etlflow.audit.noop)
}
