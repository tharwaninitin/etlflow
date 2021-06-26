package etlflow.executor

import etlflow.{JobEnv, ServerSuiteHelper}
import etlflow.api.Schema.EtlJobArgs
import etlflow.db.{EtlJob, RunDbMigration}
import zio.{RIO, ZIO}
import zio.test.Assertion.equalTo
import zio.test._

object ExecutorTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {

  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))
  def job(args: EtlJobArgs): RIO[JobEnv,EtlJob] = executor.runActiveEtlJob(args,"Test", fork = false)

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("Executor Spec")(
      testM("Test runActiveEtlJob with correct JobName") {
        assertM(job(EtlJobArgs("Job1")).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("Test runActiveEtlJob with disabled JobName") {
        assertM(job(EtlJobArgs("Job2")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Job Job2 is disabled"))
      },
      testM("Test runActiveEtlJob with incorrect JobName") {
        assertM(job(EtlJobArgs("InvalidEtlJob")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("InvalidEtlJob not present"))
      },
    ) @@ TestAspect.sequential).provideCustomLayerShared((testDBLayer ++ testJsonLayer).orDie)

}