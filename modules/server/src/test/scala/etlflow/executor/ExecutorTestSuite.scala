package etlflow.executor

import etlflow.ServerSuiteHelper
import etlflow.jdbc.DBEnv
import etlflow.utils.EtlFlowHelper.{EtlJob, EtlJobArgs}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, Semaphore, ZIO}

object ExecutorTestSuite extends DefaultRunnableSpec with Executor with ServerSuiteHelper {

  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))

  def job(args: EtlJobArgs,sem: Semaphore): RIO[DBEnv with Blocking with Clock, EtlJob] =
    runActiveEtlJob[MEJP](args,sem,config,etlJob_name_package,"Test",testJobsQueue,false)

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("Executor Spec")(
      testM("Test runActiveEtlJob with correct JobName") {
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            args    = EtlJobArgs("Job1")
            status  <- job(args,sem)
          } yield status).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done")
        )
      },
      testM("Test runActiveEtlJob with incorrect JobName") {
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            args    = EtlJobArgs("InvalidEtlJob")
            status  <- job(args,sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("InvalidEtlJob not present")
        )
      },
    ) @@ TestAspect.sequential).provideCustomLayerShared(testDBLayer.orDie)

}
