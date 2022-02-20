package etlflow.executor

import etlflow.db.{DBEnv, DBServerEnv}
import etlflow.server.model.EtlJob
import etlflow.{JobEnv, ResetServerDB, ServerSuiteHelper}
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, ZIO}

object ServerExecutorTestSuite extends ServerSuiteHelper {

  def job(name: String, props: Map[String, String] = Map.empty): RIO[JobEnv with DBServerEnv, EtlJob] =
    executor.runActiveEtlJob(name, props, "Test", fork = false)

  val spec: ZSpec[environment.TestEnvironment with JobEnv with DBEnv with DBServerEnv, Any] =
    suite("Server Executor")(
      testM("ResetDB") {
        assertM(ResetServerDB.live.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("Test runActiveEtlJob with correct JobName") {
        assertM(job("Job1").foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("Test runActiveEtlJob with disabled JobName") {
        assertM(job("Job2").foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(
          equalTo("Job Job2 is disabled")
        )
      },
      testM("Test runActiveEtlJob with incorrect JobName") {
        assertM(job("InvalidEtlJob").foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(
          equalTo("InvalidEtlJob not present")
        )
      },
      testM("Test runActiveEtlJob with deploy mode is kubernetes") {
        assertM(job("Job6").foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(
          equalTo("Deploy mode KUBERNETES not yet supported")
        )
      },
      testM("Test runActiveEtlJob with deploy mode is livy") {
        assertM(job("Job7").foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(
          equalTo("Deploy mode LIVY not yet supported")
        )
      },
      testM("Test runActiveEtlJob with deploy mode is local sub process") {
        assertM(job("Job8").foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(
          equalTo("LOCAL SUB PROCESS JOB Job8 failed with error")
        )
      }
//      testM("Test runActiveEtlJob with deploy mode is dataproc") {
//        assertM(job(EtlJobArgs("Job9",Some(List(Props("x1","x2"))))).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("invalid endpoint, expecting \"<host>:<port>\""))
//      }
    ) @@ TestAspect.sequential
}
