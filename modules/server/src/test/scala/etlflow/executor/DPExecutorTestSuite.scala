package etlflow.executor

import etlflow.SchedulerSuiteHelper
import etlflow.utils.EtlFlowHelper.EtlJobArgs
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.test.Assertion.equalTo
import zio.test._

object DPExecutorTestSuite extends DefaultRunnableSpec with Executor with SchedulerSuiteHelper {

  def job(args: EtlJobArgs, sem: Semaphore): RIO[Blocking with Clock, Unit] =
    managedTransactor.use(transactor => runDataProcJob(args,transactor,etlJob_name_package,dataproc,dp_main_class,dp_libs,sem))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("DataProc Executor Spec")(
      testM("Test DataProc Execution Job") {
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            args    = EtlJobArgs("EtlJob4", List.empty)
            status  <- job(args,sem)
          } yield status).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done")
        )
      },
      testM("Test DataProc Execution Job With Incorrect Job Details") {
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            args    = EtlJobArgs("EtlJob", List.empty)
            status  <- job(args,sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("EtlJob not present")
          )
      },
    ) @@ TestAspect.sequential
}
