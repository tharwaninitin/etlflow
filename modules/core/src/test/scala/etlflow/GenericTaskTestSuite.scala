package etlflow

import etlflow.audit.Audit
import etlflow.log.ApplicationLogger
import etlflow.task.GenericTask
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, Task, ZIO}

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object GenericTaskTestSuite extends ApplicationLogger {

  def processDataFail(): Task[Unit] = ZIO.attempt {
    logger.info("Hello World")
    throw new RuntimeException("Failed in processing data")
  }

  def processData(): Task[Unit] = ZIO.logInfo("Hello World")

  val spec: Spec[Audit, Any] =
    suite("Generic Task")(
      test("Execute GenericETLTask with error") {
        val task: RIO[Audit, Unit] = GenericTask(
          name = "ProcessData",
          task = processDataFail()
        ).toZIO
        assertZIO(task.foldZIO(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(
          equalTo("Failed in processing data")
        )
      },
      test("Execute GenericETLTask with success") {
        val task: RIO[Audit, Unit] = GenericTask(
          name = "ProcessData",
          task = processData()
        ).toZIO
        assertZIO(task.foldZIO(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(
          equalTo("ok")
        )
      }
    )
}
