package etlflow.executor

import etlflow.coretests.MyEtlJobPropsMapping
import etlflow.{EtlJobProps, SchedulerSuiteHelper}
import etlflow.etljobs.{EtlJob => CoreEtlJob}
import etlflow.utils.EtlFlowHelper.{EtlJob, EtlJobArgs}
import zio._
import zio.test.Assertion.equalTo
import zio.test._

object LocalExecutorTestSuite extends DefaultRunnableSpec with Executor with SchedulerSuiteHelper {

  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))
  type MEJP = MyEtlJobPropsMapping[EtlJobProps,CoreEtlJob[EtlJobProps]]

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Local Executor Spec")(
      testM("Test Local Job with correct JobName") {
        def job(sem: Semaphore): Task[Option[EtlJob]] = runActiveEtlJob[MEJP](EtlJobArgs("Job1", List.empty),transactor,sem,global_properties,etlJob_name_package,"Test",jobTestQueue)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done")
        )
      },
      testM("Test Local Job with correct JobName and job retry") {
        def job(sem: Semaphore): Task[EtlJob] = runLocalJob(EtlJobArgs("Job5", List.empty),transactor,etlJob_name_package,sem,fork = false,1,1)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Exception in Step")
        )
      },
      testM("Test Local Job with incorrect JobName") {
        def job(sem: Semaphore): Task[EtlJob] = runLocalJob(EtlJobArgs("EtlJob", List.empty),transactor,etlJob_name_package,sem)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Job validation failed with EtlJob not present")
        )
      },
      testM("Test validateJob with correct JobName") {
        val status = validateJob(EtlJobArgs("Job4", List.empty),etlJob_name_package)
        assertM(status.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("Test validateJob with incorrect JobName") {
        val status = validateJob(EtlJobArgs("EtlJob", List.empty),etlJob_name_package)
        assertM(status.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Job validation failed with EtlJob not present"))
      },
    ) @@ TestAspect.sequential
}
