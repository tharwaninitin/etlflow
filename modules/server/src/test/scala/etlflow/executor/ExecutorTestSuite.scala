package etlflow.executor

import etlflow.api.Schema.{EtlJobArgs, Props}
import etlflow.db.{EtlJob, RunDbMigration}
import etlflow.{JobEnv, ServerSuiteHelper}
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, ZIO}

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
      testM("Test runActiveEtlJob with deploy mode is kubernetes") {
        assertM(job(EtlJobArgs("Job6")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Deploy mode KUBERNETES not yet supported"))
      },
      testM("Test runActiveEtlJob with deploy mode is livy") {
        assertM(job(EtlJobArgs("Job7")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Deploy mode livy not yet supported"))
      },
      testM("Test runActiveEtlJob with deploy mode is local sub process") {
        assertM(job(EtlJobArgs("Job8")).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("LOCAL SUB PROCESS JOB Job8 failed with error"))
      },
//      testM("Test runActiveEtlJob with deploy mode is dataproc") {
//        assertM(job(EtlJobArgs("Job9",Some(List(Props("x1","x2"))))).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("invalid endpoint, expecting \"<host>:<port>\""))
//      }
    ) @@ TestAspect.sequential).provideCustomLayerShared((testDBLayer ++ testJsonLayer).orDie)

}