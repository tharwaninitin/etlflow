package examples

import etlflow.etltask.GenericTask
import etlflow.utils.ApplicationLogger
import zio.{ExitCode, URIO}

object Job1 extends zio.App with ApplicationLogger {

  def processData(): Unit = logger.info(s"Hello World")

  private val step1 = GenericTask(
    name = "Step_1",
    function = processData()
  )

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = step1.executeZio.provideCustomLayer(etlflow.log.noLog).exitCode
}
