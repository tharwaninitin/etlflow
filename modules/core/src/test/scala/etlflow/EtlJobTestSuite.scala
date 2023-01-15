package etlflow

import etlflow.audit.Audit
import etlflow.log.ApplicationLogger
import etlflow.task.GenericTask
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object EtlJobTestSuite extends ApplicationLogger {
  def processData(msg: String): Unit = logger.info(s"Hello World from $msg")

  def processDataFailure(msg: String): Unit = {
    logger.error(s"Hello World from $msg")
    throw new RuntimeException("!!! Failure")
  }

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

  val task2Failure: GenericTask[Unit] = GenericTask(
    name = "ProcessData2",
    function = processDataFailure("ProcessData2")
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
    suite("EtlJob Test Suite")(
      test("Test Job") {
        val pipeline    = task1 *> task2 *> task3 *> task4
        val pipelineZIO = pipeline.execute("Hello World 1", Map.empty).provide(audit.console)
        assertZIO(pipelineZIO.foldZIO(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Test Job (Failure)") {
        val pipeline    = task1 *> task2Failure *> task3 *> task4
        val pipelineZIO = pipeline.execute("Hello World 2", Map.empty).provide(audit.console)
        assertZIO(pipelineZIO.foldZIO(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("!!! Failure"))
      }
    )
}
