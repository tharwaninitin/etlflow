package examples

import etlflow.task.GenericTask
import etlflow.utils.ApplicationLogger
import zio.{ExitCode, URIO}

object Job1 extends zio.App with ApplicationLogger {

  def processData(): Unit = logger.info(s"Hello World")

  private val task1 = GenericTask(
    name = "Task_1",
    function = processData()
  )

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = task1.execute.provideCustomLayer(etlflow.log.noLog).exitCode
}
