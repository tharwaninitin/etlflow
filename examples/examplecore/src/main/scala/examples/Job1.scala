package examples

import etlflow.task.GenericTask
import etlflow.log.ApplicationLogger
import zio._

object Job1 extends zio.ZIOAppDefault with ApplicationLogger {

  override val bootstrap = zioSlf4jLogger

  def executeTask(): Unit = logger.info(s"Hello EtlFlow Task")

  private val task1 = GenericTask(
    name = "Task_1",
    function = executeTask()
  )

  override def run: Task[Unit] = task1.execute.provideLayer(etlflow.audit.noLog)
}
