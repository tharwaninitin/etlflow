package etlflow.executor

import etlflow.SchedulerSuiteHelper
import etlflow.utils.EtlFlowHelper.{EtlJob, EtlJobArgs}
import zio._
import zio.test.Assertion.equalTo
import zio.test._

object DPExecutorTestSuite extends DefaultRunnableSpec with Executor with SchedulerSuiteHelper {

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("DataProc Executor Spec")(
      testM("Test DataProc Execution Job") {
        def job(sem: Semaphore): Task[EtlJob] = runDataProcJob(EtlJobArgs("EtlJob4", List.empty),transactor,etlJob_name_package,dataproc,dp_main_class,dp_libs,sem)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done")
        )
      },
      testM("Test DataProc Execution Job With Incorrect Job Details") {
        def job(sem: Semaphore): Task[EtlJob] = runDataProcJob(EtlJobArgs("EtlJob", List.empty),transactor,etlJob_name_package,dataproc,dp_main_class,dp_libs,sem)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("EtlJob not present")
          )
      },
    ) @@ TestAspect.sequential
}
