package examples

import etlflow.log.ApplicationLogger
import etlflow.task.GenericTask
import zio._

object Job1 extends zio.ZIOAppDefault with ApplicationLogger {

  override val bootstrap = zioSlf4jLogger

  def executeTask(): Unit = logger.info(s"Hello EtlFlow Task")

  private val task1 = GenericTask(
    name = "Task_1",
    function = executeTask()
  )

  override def run: Task[Unit] = task1.toZIO.provideLayer(etlflow.audit.noop)
}
