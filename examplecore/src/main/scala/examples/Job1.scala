package examples

import etlflow.etlsteps.GenericETLStep
import etlflow.utils.ApplicationLogger
import zio.{ExitCode, URIO}

object Job1 extends zio.App with ApplicationLogger {

  def processData(ip: Unit): Unit = {
    logger.info(s"Hello World")
  }

  val step1 = GenericETLStep(
    name = "Step_1",
    transform_function = processData,
  )

  val job =
    for {
      _ <- step1.process(())
    } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = job.exitCode
}
