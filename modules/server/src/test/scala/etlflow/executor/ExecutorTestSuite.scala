package etlflow.executor

import etlflow.ServerSuiteHelper
import etlflow.api.ExecutorEnv
import etlflow.api.Schema.{EtlJob, EtlJobArgs}
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, ZIO}

object ExecutorTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {

  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))
  def job(args: EtlJobArgs): RIO[ExecutorEnv, EtlJob] = executor.runActiveEtlJob(args,"Test", fork = false)

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("Executor Spec")(
      testM("Test runActiveEtlJob with correct JobName") {
        assertM(job(EtlJobArgs("Job1")).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("Test runActiveEtlJob with incorrect JobName") {
        assertM(job(EtlJobArgs("InvalidEtlJob")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("InvalidEtlJob not present"))
      },
      testM("Test runActiveEtlJob with disabled JobName") {
        assertM(job(EtlJobArgs("Job2")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Job Job2 is disabled"))
      },
    ) @@ TestAspect.sequential).provideCustomLayerShared(testDBLayer.orDie)

}
