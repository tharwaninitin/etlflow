package etlflow

import etlflow.audit.AuditEnv
import etlflow.task.GenericTask
import etlflow.utils.ApplicationLogger
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, ZIO}

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object GenericTaskTestSuite extends ApplicationLogger {
  def processDataFail(): Unit = {
    logger.info("Hello World")
    throw new RuntimeException("Failed in processing data")
  }
  def processData(): Unit = logger.info("Hello World")

  val spec: Spec[TestEnvironment with AuditEnv, Any] =
    suite("Generic Task")(
      test("Execute GenericETLTask with error") {
        val task: RIO[AuditEnv, Unit] = GenericTask(
          name = "ProcessData",
          function = processDataFail()
        ).execute
        assertZIO(task.foldZIO(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(
          equalTo("Failed in processing data")
        )
      },
      test("Execute GenericETLTask with success") {
        val task: RIO[AuditEnv, Unit] = GenericTask(
          name = "ProcessData",
          function = processData()
        ).execute
        assertZIO(task.foldZIO(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(
          equalTo("ok")
        )
      }
    )
}
