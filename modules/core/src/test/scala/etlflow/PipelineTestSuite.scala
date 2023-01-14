package etlflow

import etlflow.audit.Audit
import etlflow.log.ApplicationLogger
import etlflow.task.{EtlTask, GenericTask}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object PipelineTestSuite extends ApplicationLogger {
  def processData(msg: String): Unit = logger.info(s"Hello World from $msg")

  def sendString(msg: String): String = {
    logger.info(s"Sent $msg")
    msg
  }

  def sendInt(msg: Int): Int = {
    logger.info(s"Sent $msg")
    msg
  }

  val task1: GenericTask[Unit] = GenericTask(
    name = "ProcessData1",
    function = processData("ProcessData1")
  )

  val task2: GenericTask[Unit] = GenericTask(
    name = "ProcessData2",
    function = processData("ProcessData2")
  )

  val task3: GenericTask[String] = GenericTask(
    name = "ProcessData3",
    function = sendString("ProcessData3")
  )

  val task4: GenericTask[Int] = GenericTask(
    name = "ProcessData4",
    function = sendInt(100)
  )

  val spec: Spec[Audit, Any] =
    suite("Generic Task")(
      test("Test GenericETLTask pipeline") {
        logger.info(s"EtlTask.taskSet ${EtlTask.taskSet.get()}")
        val pipeline = task1 *> task2 *> task3 *> task4
        logger.info(s"EtlTask.taskSet ${EtlTask.taskSet.get()}")
        logger.info(s"Task Name ${pipeline.name}")
        val pipelineZIO = pipeline.execute.provide(audit.console)
        assertZIO(pipelineZIO.foldZIO(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Test GenericETLTask for pipeline") {
        logger.info(s"EtlTask.taskSet ${EtlTask.taskSet.get()}")
        val pipeline = for {
          _   <- task1
          _   <- task2
          _   <- task3
          op4 <- task4
        } yield op4
        logger.info(s"EtlTask.taskSet ${EtlTask.taskSet.get()}")
        logger.info(s"Task Name ${pipeline.name}")
        val pipelineZIO = pipeline.execute.provide(audit.console)
        assertZIO(pipelineZIO.foldZIO(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      }
    )
}
