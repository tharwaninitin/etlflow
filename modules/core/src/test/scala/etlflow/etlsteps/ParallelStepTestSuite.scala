package etlflow.etlsteps

import etlflow.coretests.TestSuiteHelper
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object ParallelStepTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  def processData(ip: Unit): Unit = {
    logger.info("Hello World")
  }

  val step1 = GenericETLStep(
    name = "ProcessData",
    transform_function = processData,
  )

  val step2 = GenericETLStep(
    name = "ProcessData",
    transform_function = processData,
  )

  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Parallel Step")(
      testM("Execute Parallel step") {
        def processData(ip: Unit): Unit = {
          logger.info("Hello World")
        }

        val step1 = GenericETLStep(
          name = "ProcessData",
          transform_function = processData,
        )

        val step2 = GenericETLStep(
          name = "ProcessData",
          transform_function = processData,
        )

        val parstep = ParallelETLStep("ParallelStep")(step1, step2)

        val job = for {
          _ <- parstep.process(()).provideCustomLayer(fullLayer)
        } yield ()
        assertM(job.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("ok")))(equalTo("ok"))
      },
      test("Execute getStepProperties") {
        val parstep = ParallelETLStep("ParallelStep")(step1, step2)
        parstep.job_run_id = "123"
        val props = parstep.getStepProperties()
        assert(props)(equalTo(Map("parallel_steps" -> "ProcessData,ProcessData", "step_run_id" -> "123")))
      }
    )
}
