package etlflow.executor

import etlflow.{CoreEnv, EtlJobProps}
import etlflow.coretests.{MyEtlJobPropsMapping, TestSuiteHelper}
import etlflow.etljobs.EtlJob
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

object LocalExecutorTestSuite {
  type MEJP = MyEtlJobPropsMapping[EtlJobProps,EtlJob[EtlJobProps]]
  val jobStepProps = LocalExecutor[MEJP]().showJobStepProps("Job1", Map.empty)
  val jobProps = LocalExecutor[MEJP]().showJobProps("Job1")
  val executeJob = LocalExecutor[MEJP]().executeJob("Job1", Map.empty)

  val spec: ZSpec[environment.TestEnvironment with CoreEnv, Any] =
    suite("Local Executor")(
      testM("showJobStepProps") {
        assertM(jobStepProps.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("showJobProps") {
        assertM(jobProps.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("executeJob") {
        assertM(executeJob.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      }
    ) @@ TestAspect.sequential
}
