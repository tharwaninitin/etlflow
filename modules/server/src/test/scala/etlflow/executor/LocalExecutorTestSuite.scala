package etlflow.executor

import etlflow.SchedulerSuiteHelper
import etlflow.utils.EtlFlowHelper.{EtlJob, EtlJobArgs}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.test.Assertion.equalTo
import zio.test._

object LocalExecutorTestSuite extends DefaultRunnableSpec with Executor with SchedulerSuiteHelper {

  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))
  def job(args: EtlJobArgs, sem: Semaphore): RIO[Blocking with Clock, Unit] =
    managedTransactor.use(transactor => runLocalJob(args,transactor,etlJob_name_package,sem,fork = false,1,1))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Local Executor Spec")(
      testM("Test Local Job with correct JobName and job retry") {

        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            args    = EtlJobArgs("Job5", List.empty)
            status  <- job(args,sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Exception in Step")
        )
      },
      testM("Test Local Job with incorrect JobName") {
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            args    = EtlJobArgs("EtlJob", List.empty)
            status  <- job(args,sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Job validation failed with EtlJob not present")
        )
      },
    ) @@ TestAspect.sequential
}
