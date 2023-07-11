package examples

import etlflow.log.ApplicationLogger
import etlflow.task.GenericTask
import zio.{Task, ZIO}

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object Job2 extends zio.ZIOAppDefault with ApplicationLogger {

  override val bootstrap = zioSlf4jLogger

  private def processData1(): Task[String] = ZIO.attempt {
    logger.info(s"Hello World")
    "Hello World"
  }

  private val task1 = GenericTask(
    name = "Task_1",
    task = processData1()
  )

  private def processData2(): Task[Unit] = ZIO.logInfo("Hello World")

  private val task2 = GenericTask(
    name = "Task_2",
    task = processData2()
  )

  private def processData3(): Task[Unit] = ZIO.attempt {
    logger.info(s"Hello World")
    throw new RuntimeException("Error123")
  }

  private val task3 = GenericTask(
    name = "Task_3",
    task = processData3()
  )

  private val job = for {
    _ <- task1.toZIO
    _ <- task2.toZIO
    _ <- task3.toZIO
  } yield ()

  override def run: Task[Unit] = job.provideLayer(etlflow.audit.noop)
}
