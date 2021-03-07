package etlflow.executor

import etlflow.SchedulerSuiteHelper
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.utils.EtlFlowHelper.{EtlJob, EtlJobArgs}
import zio._
import zio.test.Assertion.equalTo
import zio.test._

object LocalSubProcessExecutorTestSuite extends DefaultRunnableSpec with Executor with SchedulerSuiteHelper {

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Local Sub-Process Executor Spec")(
      testM("Test Local Sub-Process Job with correct JobName") {
        def job(sem: Semaphore): Task[EtlJob] = runLocalSubProcessJob(EtlJobArgs("Job2LocalJobGenericStep", List.empty),transactor,etlJob_name_package,MyEtlJobPropsMapping.local_subprocess,sem,fork = false)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done")
        )
      },
      testM("Test Local Sub-Process with incorrect JobName") {
        def job(sem: Semaphore): Task[EtlJob] = runLocalSubProcessJob(EtlJobArgs("EtlJob10", List.empty),transactor,etlJob_name_package,MyEtlJobPropsMapping.local_subprocess,sem)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("EtlJob10 not present")
        )
      }
    ) @@ TestAspect.sequential
}
