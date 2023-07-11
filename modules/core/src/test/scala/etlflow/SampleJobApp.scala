package etlflow

import etlflow.audit.Audit
import etlflow.task.GenericTask
import zio.{Chunk, Layer, RIO, Task, UIO, ZIO}

@SuppressWarnings(Array("org.wartremover.warts.ToString"))
object SampleJobApp extends JobApp {

  override val auditLayer: Layer[Throwable, Audit] = audit.console

  def processData1(): Task[String] = ZIO.succeed {
    logger.info("Hello World")
    // Thread.sleep(2000)
    "Hello World 1"
  }

  private val task1 = GenericTask(
    name = "Task_1",
    task = processData1()
  )

  def processData2(): UIO[Unit] = ZIO.logInfo("Hello World 2")

  private val task2 = GenericTask(
    name = "Task_2",
    task = processData2()
  )

  def processData3(): UIO[Unit] = ZIO.logInfo("Hello World 3")

  private val task3 = GenericTask(
    name = "Task_3",
    task = processData3()
  )

  private val job = for {
    _ <- task1.toZIO
    _ <- task2.toZIO
    _ <- task3.toZIO
  } yield ()

  override def job(args: Chunk[String]): RIO[Audit, Unit] = job
}
