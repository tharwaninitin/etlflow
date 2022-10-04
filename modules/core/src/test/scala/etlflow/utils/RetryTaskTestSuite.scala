package etlflow.utils

import etlflow.audit.LogEnv
import etlflow.model.EtlFlowException.RetryException
import etlflow.task.GenericTask
import zio.{Duration => ZDuration}
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, ZIO}
import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object RetryTaskTestSuite extends ApplicationLogger {
  val spec: Spec[TestEnvironment with LogEnv, Any] =
    suite("Retry Task")(
      test("Execute GenericETLTask with retry") {
        def processDataFail(): Unit = {
          logger.info("Hello World")
          throw RetryException("Failed in processing data")
        }

        val task: RIO[LogEnv, Unit] = GenericTask(
          name = "ProcessData",
          function = processDataFail()
        ).execute.retry(RetrySchedule(2, 5.second))

        val program = for {
          s <- task.fork
          _ <- TestClock.adjust(ZDuration.fromScala(30.second))
          _ <- s.join
        } yield ()

        assertZIO(program.foldZIO(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(
          equalTo("Failed in processing data")
        )
      }
    )
}
