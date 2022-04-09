package etlflow

import etlflow.log.LogEnv
import etlflow.task.GenericTask
import zio.{RIO, ZEnv, ZLayer}

object SampleJobApp extends JobApp {

  override val logLayer: ZLayer[ZEnv, Throwable, LogEnv] = log.Memory.live(java.util.UUID.randomUUID.toString)

  def processData1(): String = {
    logger.info("Hello World")
    // Thread.sleep(2000)
    "Hello World"
  }

  private val task1 = GenericTask(
    name = "Task_1",
    function = processData1()
  )

  def processData2(): Unit =
    logger.info("Hello World")

  private val task2 = GenericTask(
    name = "Task_2",
    function = processData2()
  )

  def processData3(): Unit =
    logger.info("Hello World")
  // throw new RuntimeException("Error123")

  private val task3 = GenericTask(
    name = "Task_3",
    function = processData3()
  )

  private val job = for {
    _ <- task1.execute
    _ <- task2.execute
    _ <- task3.execute
  } yield ()

  override def job(args: List[String]): RIO[LogEnv, Unit] = job
}
