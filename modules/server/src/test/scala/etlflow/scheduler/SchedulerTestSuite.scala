package etlflow.scheduler

import etlflow.ServerSuiteHelper
import etlflow.db.{EtlJob, RunDbMigration}
import zio.duration.{Duration => ZDuration}
import zio.test._
import zio.test.environment.TestClock

import scala.concurrent.duration.{Duration, MINUTES}

object SchedulerTestSuite extends DefaultRunnableSpec with ServerSuiteHelper with Scheduler {

  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))
  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("Rest Scheduler Suite")(
      testM("Test scheduler with Job1")(
        for {
          jobs     <- getEtlJobs[MEJP](ejpm_package).map(jl => jl.filter(_.name == "Job1"))
          fiber    <- etlFlowScheduler(jobs).fork
          _        <- TestClock.adjust(ZDuration.fromScala(Duration(3,MINUTES)))
          _        <- fiber.interrupt
        } yield assertCompletes
      ),
      testM("Test scheduler with no jobs")(
        etlFlowScheduler(List.empty).as(assertCompletes)
      ),
      testM("Test scheduler with  jobs")(
        etlFlowScheduler(List(EtlJob("Job1", Map.empty))).as(assertCompletes)
      )
    ) @@ TestAspect.sequential).provideCustomLayerShared((fullLayer).orDie)
}

