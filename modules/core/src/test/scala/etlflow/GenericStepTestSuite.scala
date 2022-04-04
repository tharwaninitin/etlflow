package etlflow

import etlflow.etltask.GenericTask
import etlflow.utils.ApplicationLogger
import zio.test._
import scala.util.Try

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object GenericStepTestSuite extends ApplicationLogger {
  def processDataFail(): Unit = {
    logger.info("Hello World")
    throw new RuntimeException("Failed in processing data")
  }
  def processData(): Unit =
    logger.info("Hello World")

  def spec(log: etlflow.log.Service[Try]): ZSpec[environment.TestEnvironment, Any] =
    suite("Generic Step")(
      test("Execute GenericETLStep with error") {
        val task: Try[Unit] = GenericTask(
          name = "ProcessData",
          function = processDataFail()
        ).executeTry(log)
        assertTrue(task.isFailure)
      },
      test("Execute GenericETLStep with success") {
        val task: Try[Unit] = GenericTask(
          name = "ProcessData",
          function = processData()
        ).executeTry(log)

        assertTrue(task.isSuccess)
      }
    )
}
