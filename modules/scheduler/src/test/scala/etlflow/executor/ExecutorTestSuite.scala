package etlflow.executor

import etlflow.jdbc.DbManager
import etlflow.scheduler._
import etlflow.scheduler.api.EtlFlowHelper.{EtlJobArgs, Props}
import etlflow.scheduler.schema.MyEtlJobProps
import zio._
import zio.test.Assertion.equalTo
import zio.test._

object ExecutorTestSuite extends DefaultRunnableSpec with ExecutorHelper with TestSuiteHelper with DbManager with TestSchedulerApp {

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Executor Spec")(
      testM("Test DataProc Execution Job") {
        val status = runDataprocJob(EtlJobArgs("EtlJobBarcWeekMonthToDate", List.empty),transactor,dataproc,main_class,dp_libs,etlJob_name_package)
        assertM(status.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("Test DataProc Execution Job With Incorrect Job Details") {
        val status = runDataprocJob(EtlJobArgs("EtlJobBarcWeekMonthTo", List.empty),transactor,dataproc,main_class,dp_libs,etlJob_name_package)
        assertM(status.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("EtlJobBarcWeekMonthTo not present"))
      },
      testM("Test Local SubProcess Execution Job") {
        val status = runLocalSubProcessJob(EtlJobArgs("EtlJob4BQtoBQ", List(Props("",""))),transactor,etlJob_name_package,MyEtlJobProps.local_subprocess)
        assertM(status.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      },
      testM("Test Local SubProcess  Execution Job with Incorrect Job Details") {
        val status = runLocalSubProcessJob(EtlJobArgs("EtlJob", List.empty),transactor,etlJob_name_package,MyEtlJobProps.local_subprocess)
        assertM(status.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("EtlJob not present"))
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
