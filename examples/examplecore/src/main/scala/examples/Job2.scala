package examples

import etlflow.task.GenericTask
import etlflow.utils.ApplicationLogger
import zio.{ExitCode, URIO}

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object Job2 extends zio.App with ApplicationLogger {

  def processData1(): String = {
    logger.info(s"Hello World")
    "Hello World"
  }

  private val step1 = GenericTask(
    name = "Step_1",
    function = processData1()
  )

  def processData2(): Unit = logger.info("Hello World")

  private val step2 = GenericTask(
    name = "Step_2",
    function = processData2()
  )

  def processData3(): Unit = {
    logger.info(s"Hello World")
    throw new RuntimeException("Error123")
  }

  private val step3 = GenericTask(
    name = "Step_3",
    function = processData3()
  )

  private val job = for {
    _ <- step1.executeZio
    _ <- step2.executeZio
    _ <- step3.executeZio
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = job.provideCustomLayer(etlflow.log.noLog).exitCode
}
