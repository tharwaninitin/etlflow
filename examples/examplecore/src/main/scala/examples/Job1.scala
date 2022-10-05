package examples

import etlflow.task.GenericTask
import etlflow.utils.ApplicationLogger
import zio.Task

object Job1 extends zio.ZIOAppDefault with ApplicationLogger {

  def processData(): Unit = logger.info(s"Hello World")

  private val task1 = GenericTask(
    name = "Task_1",
    function = processData()
  )

  override def run: Task[Unit] = task1.execute.provideLayer(etlflow.audit.noLog)
}
