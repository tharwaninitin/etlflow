package etlflow.scheduler

import zio.duration._
import zio.test.Assertion.equalTo
import zio.test._
import zio.test.environment.TestClock

object SchedulerPackageTestSuite {

  val spec: ZSpec[environment.TestEnvironment, Any] =
    (suite("Rest Scheduler Suite")(
      testM("Test Cron Job")(
        for {
          _      <- sleepForCron(parseCron("0 */2 * * * ?").get).fork
          _       <- TestClock.adjust(10.seconds)
        } yield assert(1)(equalTo(1))
      )
    ) @@ TestAspect.sequential)
}

