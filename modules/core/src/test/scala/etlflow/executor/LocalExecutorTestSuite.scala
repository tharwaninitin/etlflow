package etlflow.executor

import etlflow.coretests.TestSuiteHelper
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object LocalExecutorTestSuite extends DefaultRunnableSpec with TestSuiteHelper {

  val jobStepProps = LocalExecutor(ejpm_package).showJobStepProps("Job1", Map.empty)

  val jobProps = LocalExecutor(ejpm_package).showJobProps("Job1")

  val executeJob = LocalExecutor(ejpm_package).executeJob("Job1", Map.empty)

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("Executor Spec")(
      testM("showJobStepProps") {
        assertM(jobStepProps.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("showJobProps") {
        assertM(jobProps.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("executeJob") {
        assertM(executeJob.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      }
    ) @@ TestAspect.sequential).provideCustomLayer(fullLayer.orDie)
}