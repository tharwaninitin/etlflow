package etlflow.executor

import etlflow.SchedulerSuiteHelper
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.utils.EtlFlowHelper.{EtlJob, EtlJobArgs}
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.test.Assertion.equalTo
import zio.test._

object LocalSubProcessExecutorTestSuite extends DefaultRunnableSpec with Executor with SchedulerSuiteHelper {

  def job(args: EtlJobArgs, sem: Semaphore): RIO[Blocking with Clock, Unit] =
    managedTransactor.use(transactor => runLocalSubProcessJob(EtlJobArgs("Job2LocalJobGenericStep", List.empty),transactor,etlJob_name_package,MyEtlJobPropsMapping.local_subprocess,sem,fork = false))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Local Sub-Process Executor Spec")(
      testM("Test Local Sub-Process Job with correct JobName") {
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            args    = EtlJobArgs("Job2", List.empty)
            status  <- job(args,sem)
          } yield status).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done")
        )
      },
      testM("Test Local Sub-Process with incorrect JobName") {
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            args    = EtlJobArgs("InvalidJob", List.empty)
            status  <- job(args,sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("InvalidJob not present")
        )
      }
    ) @@ TestAspect.sequential
}
