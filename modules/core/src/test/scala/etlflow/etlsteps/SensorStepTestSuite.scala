package etlflow.etlsteps

import etlflow.coretests.TestSuiteHelper
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM, environment}
import scala.concurrent.duration._

object SensorStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper with SensorStep {
  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlowJobStepTestSuite")(
      testM("Execute EtlFlowJobStep") {

        def processDataFail(ip: Unit): Unit = {
          logger.info("Hello World")
          throw new RuntimeException("Failed in processing data")
        }

        val step = GenericETLStep(
          name = "ProcessData",
          transform_function = processDataFail,
        ).process().retry(noThrowable && schedule(1, 5.second)).provideCustomLayer(fullLayer)

        assertM(step.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("Failed in processing data"))
      }
    )
}