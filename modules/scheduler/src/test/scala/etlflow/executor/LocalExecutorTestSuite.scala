package etlflow.executor

import etlflow.utils.EtlFlowHelper.{EtlJob, EtlJobArgs, Props}
import etlflow.schema.MyEtlJobProps
import etlflow.SchedulerSuiteHelper
import zio._
import zio.test.Assertion.equalTo
import zio.test._

object LocalExecutorTestSuite extends DefaultRunnableSpec with ExecutorHelper with SchedulerSuiteHelper {

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Executor Spec")(
      testM("Test Local SubProcess Job") {
        def job(sem: Semaphore): Task[EtlJob] = runLocalSubProcessJob(EtlJobArgs("EtlJob4BQtoBQ", List(Props("",""))),transactor,etlJob_name_package,MyEtlJobProps.local_subprocess,sem)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done")
        )
      },
      testM("Test Local SubProcess Job with Incorrect Job Details") {
        def job(sem: Semaphore): Task[EtlJob] = runLocalSubProcessJob(EtlJobArgs("EtlJob", List.empty),transactor,etlJob_name_package,MyEtlJobProps.local_subprocess,sem)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("EtlJob not present")
        )
      },
      testM("Test validateJob with Incorrect Job Details") {
        val status = validateJob(EtlJobArgs("EtlJob", List.empty),etlJob_name_package)
        assertM(status.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("EtlJob not present"))
      },
      testM("Test validateJob with Correct Job Details") {
        val status = validateJob(EtlJobArgs("EtlJob4BQtoBQ", List.empty),etlJob_name_package)
        assertM(status.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      }
    ) @@ TestAspect.sequential
}
