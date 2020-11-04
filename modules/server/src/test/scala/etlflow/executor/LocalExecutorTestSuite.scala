package etlflow.executor

import etlflow.SchedulerSuiteHelper
import etlflow.MyEtlJobName
import etlflow.utils.EtlFlowHelper.{EtlJob, EtlJobArgs}
import etlflow.webserver.api.Http4sTestSuite.{credentials, runDbMigration}
import zio._
import zio.test.Assertion.equalTo
import zio.test._

object LocalExecutorTestSuite extends DefaultRunnableSpec with Executor with SchedulerSuiteHelper {



  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Local Executor Spec")(
      testM("Test Local Job with correct JobName") {
        zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))
        def job(sem: Semaphore): Task[Option[EtlJob]] = runActiveEtlJob(EtlJobArgs("EtlJob4", List.empty),transactor,sem,global_properties,etlJob_name_package,"Test",jobTestQueue)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done")
        )
      },
      testM("Test Local Job with incorrect JobName") {
        def job(sem: Semaphore): Task[EtlJob] = runLocalJob(EtlJobArgs("EtlJob", List.empty),transactor,etlJob_name_package,sem)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("EtlJob not present")
        )
      },
//      testM("Test Local SubProcess Job with correct JobName") {
//        def job(sem: Semaphore): Task[EtlJob] =
//          runLocalSubProcessJob(EtlJobArgs("EtlJob4LocalSubProcess", List.empty),transactor,etlJob_name_package,MyEtlJobName.local_subprocess,sem, fork = false)
//        assertM(
//          (for {
//            sem     <- Semaphore.make(permits = 1)
//            status  <- job(sem)
//            _ = println(status)
//          } yield status).foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done")
//        )
//      },
      testM("Test Local SubProcess Job with incorrect JobName") {
        def job(sem: Semaphore): Task[EtlJob] = runLocalSubProcessJob(EtlJobArgs("EtlJob", List.empty),transactor,etlJob_name_package,MyEtlJobName.local_subprocess,sem)
        assertM(
          (for {
            sem     <- Semaphore.make(permits = 1)
            status  <- job(sem)
          } yield status).foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("EtlJob not present")
        )
      },
      testM("Test validateJob with correct JobName") {
        val status = validateJob(EtlJobArgs("EtlJob4", List.empty),etlJob_name_package)
        assertM(status.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("Test validateJob with incorrect JobName") {
        val status = validateJob(EtlJobArgs("EtlJob", List.empty),etlJob_name_package)
        assertM(status.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("EtlJob not present"))
      },
    ) @@ TestAspect.sequential
}
