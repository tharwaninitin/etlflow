package etlflow.etlsteps

import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._
import scala.concurrent.duration._

object SensorStepTestSuite extends DefaultRunnableSpec with SensorStep {
  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Sensor Step")(
      testM("Execute GenericETLStep with retry") {
        def processDataFail(ip: Unit): Unit = {
          logger.info("Hello World")
          throw new RuntimeException("Failed in processing data")
        }
        val step = GenericETLStep(
          name = "ProcessData",
          transform_function = processDataFail,
        ).process(()).retry(noThrowable && schedule(1, 5.second))
        assertM(step.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("Failed in processing data"))
      }
    )
}
