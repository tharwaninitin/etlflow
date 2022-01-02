package etlflow

import etlflow.core.CoreLogEnv
import etlflow.etlsteps.GenericETLStep
import etlflow.log.LogEnv
import zio.{RIO, ZEnv, ZLayer}

object SampleJobApp extends JobApp {

  override val log_layer: ZLayer[ZEnv, Throwable, LogEnv] = log.Memory.live(java.util.UUID.randomUUID.toString)

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
      op <- step1.execute(())
      _ <- step2.execute(op)
      _ <- step3.execute(())
    } yield ()

  override def job(args: List[String]): RIO[CoreLogEnv, Unit] = job
}
