package etlflow

import etlflow.etlsteps.GenericETLStep
import etlflow.log.LogEnv
import zio.{RIO, ZEnv, ZLayer}

object SampleJobApp extends JobApp {

  override val log_layer: ZLayer[ZEnv, Throwable, LogEnv] = log.Memory.live(java.util.UUID.randomUUID.toString)

  def processData1(): String = {
    logger.info("Hello World")
    Thread.sleep(2000)
    "Hello World"
  }

  val step1 = GenericETLStep(
    name = "Step_1",
    function = processData1()
  )

  def processData2(): Unit =
    logger.info("Hello World")

  val step2 = GenericETLStep(
    name = "Step_2",
    function = processData2()
  )

  def processData3(): Unit =
    logger.info("Hello World")
  // throw new RuntimeException("Error123")

  val step3 = GenericETLStep(
    name = "Step_3",
    function = processData3()
  )

  val job =
    for {
      _ <- step1.execute
      _ <- step2.execute
      _ <- step3.execute
    } yield ()

  override def job(args: List[String]): RIO[LogEnv, Unit] = job
}
