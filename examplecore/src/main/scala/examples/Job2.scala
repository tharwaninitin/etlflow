package examples

import etlflow.etlsteps.GenericETLStep
import etlflow.utils.ApplicationLogger
import zio.{ExitCode, URIO}

object Job2 extends zio.App with ApplicationLogger {

  def processData1(ip: Unit): String = {
    logger.info(s"Hello World")
    "Hello World"
  }

  val step1 = GenericETLStep(
    name = "Step_1",
    transform_function = processData1,
  )

  def processData2(ip: String): Unit = {
    logger.info(s"Hello World => $ip")
  }

  val step2 = GenericETLStep(
    name = "Step_2",
    transform_function = processData2,
  )

  def processData3(ip: Unit): Unit = {
    logger.info(s"Hello World")
    throw new RuntimeException("Error123")
  }

  val step3 = GenericETLStep(
    name = "Step_3",
    transform_function = processData3,
  )

  val job =
    for {
      op <- step1.process(())
      _ <- step2.process(op)
      _ <- step3.process(())
    } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = job.exitCode
}
