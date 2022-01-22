package etlflow.utils

import etlflow.etlsteps.GenericETLStep
import etlflow.model.EtlFlowException.RetryException
import zio.clock.Clock
import zio.duration.{Duration => ZDuration}
import zio.test.Assertion.equalTo
import zio.test.environment.TestClock
import zio.test.{assertM, environment, suite, testM, ZSpec}
import zio.{RIO, ZIO}
import scala.concurrent.duration._

object RetryStepTestSuite extends ApplicationLogger {
  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Retry Step")(
      testM("Execute GenericETLStep with retry") {
        def processDataFail(): Unit = {
          logger.info("Hello World")
          throw RetryException("Failed in processing data")
        }

        val step: RIO[Clock, Unit] = GenericETLStep(
          name = "ProcessData",
          function = processDataFail()
        ).process.retry(RetrySchedule(2, 5.second))

        val program = for {
          s <- step.fork
          _ <- TestClock.adjust(ZDuration.fromScala(15.second))
          _ <- s.join
        } yield ()

        assertM(program.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(
          equalTo("Failed in processing data")
        )
      }
    )
}
