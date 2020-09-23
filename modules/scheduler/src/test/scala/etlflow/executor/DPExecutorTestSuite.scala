package etlflow.executor

import etlflow.SchedulerSuiteHelper
import etlflow.utils.EtlFlowHelper.{EtlJob, EtlJobArgs}
import zio._
import zio.test.Assertion.equalTo
import zio.test._

object DPExecutorTestSuite extends DefaultRunnableSpec with ExecutorHelper with SchedulerSuiteHelper {

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Executor Spec")(
      testM("Test DataProc Execution Job") {
        def job(sem: Semaphore): Task[EtlJob] = runDataprocJob(EtlJobArgs("EtlJobBarcWeekMonthToDate", List.empty),transactor,dataproc,dp_main_class,dp_libs,etlJob_name_package,sem)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done")
        )
      },
      testM("Test DataProc Execution Job With Incorrect Job Details") {
        def job(sem: Semaphore): Task[EtlJob] = runDataprocJob(EtlJobArgs("EtlJobBarcWeekMonthTo", List.empty),transactor,dataproc,dp_main_class,dp_libs,etlJob_name_package,sem)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("EtlJobBarcWeekMonthTo not present")
          )
      },
    ) @@ TestAspect.sequential
}
