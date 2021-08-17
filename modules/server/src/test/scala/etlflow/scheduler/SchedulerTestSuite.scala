package etlflow.scheduler

import etlflow.ServerSuiteHelper
import etlflow.api.ServerEnv
import etlflow.db.EtlJob
import etlflow.utils.{ReflectAPI => RF}
import zio.duration.{Duration => ZDuration}
import zio.test._
import zio.duration._
import zio.test.Assertion.equalTo
import zio.test.environment.TestClock
import scala.concurrent.duration.{Duration, MINUTES}

object SchedulerTestSuite extends ServerSuiteHelper with Scheduler {
  val spec: ZSpec[environment.TestEnvironment with TestClock with ServerEnv, Any] =
    (suite("Rest Scheduler Suite")(
      testM("Test scheduler with Job1")(
        for {
          jobs     <- RF.getJobs[MEJP].map(jl => jl.filter(_.name == "Job1"))
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
      ),
      testM("Test Cron Job")(
        for {
          _      <- sleepForCron(parseCron("0 */2 * * * ?").get).fork
          _       <- TestClock.adjust(10.seconds)
        } yield assert(1)(equalTo(1))
      )
    ) @@ TestAspect.sequential)
}

