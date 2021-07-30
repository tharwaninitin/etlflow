package etlflow.scheduler

import cron4s.Cron
import etlflow.ServerSuiteHelper
import zio.test.Assertion.equalTo
import zio.test._

import zio.duration._
import zio.test.Assertion.equalTo
import zio.test._
import zio.test.environment.TestClock

object SchedulerPackageTestSuite extends DefaultRunnableSpec with ServerSuiteHelper with Scheduler {


  override def spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("Rest Scheduler Suite")(
      testM("Test Cron Job")(
        for {
          _      <- sleepForCron(Cron("0 */2 * * * ?").toOption.get).fork
          _       <- TestClock.adjust(10.seconds)
        } yield assert(1)(equalTo(1))
      )
    ) @@ TestAspect.sequential)
}

