package etlflow.executor

import etlflow.{EtlJobProps, SchedulerSuiteHelper}
import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.utils.EtlFlowHelper.{EtlJob, EtlJobArgs}
import zio.test.Assertion.equalTo
import zio.{Semaphore, Task, ZIO}
import zio.test.{DefaultRunnableSpec, TestAspect, ZSpec, assertM, environment, suite, testM}

object ExecutorTestSuite extends DefaultRunnableSpec with Executor with SchedulerSuiteHelper {

  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))
  type MEJP = MyEtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]
  def job(args: EtlJobArgs,sem: Semaphore): Task[EtlJob] =
    managedTransactor.use(transactor => runActiveEtlJob[MEJP](args,transactor,sem,global_properties,etlJob_name_package,"Test",jobTestQueue))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Executor Spec")(
      testM("Test runActiveEtlJob with correct JobName") {
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            args    = EtlJobArgs("Job1", List.empty)
            status  <- job(args,sem)
          } yield status).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done")
        )
      },
      testM("Test runActiveEtlJob with incorrect JobName") {
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            args    = EtlJobArgs("InvalidEtlJob", List.empty)
            status  <- job(args,sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("InvalidEtlJob not present")
        )
      },
//      testM("Test validateJob with correct JobName") {
//        val status = validateJob(EtlJobArgs("Job4", List.empty),etlJob_name_package)
//        assertM(status.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
//      },
//      testM("Test validateJob with incorrect JobName") {
//        val status = validateJob(EtlJobArgs("InvalidEtlJob", List.empty),etlJob_name_package)
//        assertM(status.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Job validation failed with InvalidEtlJob not present"))
//      },
    ) @@ TestAspect.sequential

}
