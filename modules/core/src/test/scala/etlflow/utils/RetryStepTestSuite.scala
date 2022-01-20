package etlflow.utils

import etlflow.etlsteps.GenericETLStep
import etlflow.model.EtlFlowException.RetryException
import zio.clock.Clock
import zio.test.Assertion.equalTo
import zio.test.{ZSpec, assertM, environment, suite, testM}
import zio.{Task, ZIO}
import scala.concurrent.duration._

object RetryStepTestSuite extends ApplicationLogger {
  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Retry Step")(
      testM("Execute GenericETLStep with retry") {
        def processDataFail(ip: Unit): Unit = {
          logger.info("Hello World")
          throw RetryException("Failed in processing data")
        }
        val step: Task[Unit] = GenericETLStep(
          name = "ProcessData",
          transform_function = processDataFail
        ).process(()).retry(RetrySchedule(2, 5.second)).provideLayer(Clock.live)
        assertM(step.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("Failed in processing data"))
      }
    )
}
