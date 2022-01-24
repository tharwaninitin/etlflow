package examples

import etlflow.etlsteps.GenericETLStep
import etlflow.utils.ApplicationLogger
import zio.{ExitCode, URIO}

object Job1 extends zio.App with ApplicationLogger {

  def processData(): Unit = logger.info(s"Hello World")

  val step1 = GenericETLStep(
    name = "Step_1",
    function = processData()
  )

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = step1.process.exitCode
}
