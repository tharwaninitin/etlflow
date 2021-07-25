package etlflow.coretests.steps.steps.parallel

import etlflow.coretests.TestSuiteHelper
import etlflow.etlsteps.{GenericETLStep, ParallelETLStep}
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, ZSpec, assertM, environment}

object ParallelStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper {


  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("EtlFlow")(
      suite("Parallel Step")(
        testM("Execute Parallel step") {
          def processData(ip: Unit): Unit = {
            logger.info("Hello World")
          }

          val step1 = GenericETLStep(
            name               = "ProcessData",
            transform_function = processData,
          )

          val step2 = GenericETLStep(
            name               = "ProcessData",
            transform_function = processData,
          )

          val parstep = ParallelETLStep("ParallelStep")(step1,step2)

          val job = for {
            _ <- parstep.process().provideCustomLayer(fullLayer)
          } yield ()
          assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
        }
      )
    )
}


