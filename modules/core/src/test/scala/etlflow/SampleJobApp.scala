package etlflow

import etlflow.audit.Audit
import etlflow.task.GenericTask
import zio.{Chunk, Layer, RIO}

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
object SampleJobApp extends JobApp {

  override val auditLayer: Layer[Throwable, Audit] = audit.memory(java.util.UUID.randomUUID.toString)

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
    _ <- task1.toZIO
    _ <- task2.toZIO
    _ <- task3.toZIO
  } yield ()

  override def job(args: Chunk[String]): RIO[Audit, Unit] = job
}
