package etlflow.utils

import etlflow.audit.Audit
import etlflow.log.ApplicationLogger
import etlflow.model.EtlFlowException.RetryException
import etlflow.task.GenericTask
import zio.test.Assertion.equalTo
import zio.test._
import zio.{Duration => ZDuration, RIO, ZIO}
import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object RetryTaskTestSuite extends ApplicationLogger {
  val spec: Spec[Audit, Any] =
    suite("Retry Task")(
      test("Execute GenericETLTask with retry") {
        def processDataFail(): Unit = {
          logger.info("Hello World")
          throw RetryException("Failed in processing data")
        }

        val task: RIO[Audit, Unit] = GenericTask(
          name = "ProcessData",
          function = processDataFail()
        ).execute.retry(RetrySchedule.recurs(2, 5.second))

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
