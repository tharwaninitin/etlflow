package etlflow.coretests.executor

import etlflow.coretests.TestSuiteHelper
import etlflow.coretests.steps.db.DBStepTestSuite.fullLayer
import etlflow.executor.LocalExecutor
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object LocalExecutorTestSuite  extends DefaultRunnableSpec with TestSuiteHelper  {

  val jobStepProps = LocalExecutor(ejpm_package).showJobStepProps("Job1", Map.empty,ejpm_package)

  val jobProps = LocalExecutor(ejpm_package).showJobProps("Job1", Map.empty,ejpm_package)

  val executeJob = LocalExecutor(ejpm_package,Some("123"),Some("true")).executeJob("Job1", Map.empty)

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
    )@@ TestAspect.sequential).provideCustomLayer(fullLayer.orDie)
}
