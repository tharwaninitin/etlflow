package etlflow.scheduler

import etlflow.{ServerSuiteHelper, TestApiImplementation}
import zio.test._
import zio.test.environment.TestClock
import zio.duration.{Duration => ZDuration}
import scala.concurrent.duration.{Duration, MINUTES}

object SchedulerTestSuite extends DefaultRunnableSpec with TestApiImplementation with ServerSuiteHelper with Scheduler {

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Rest Scheduler Suite")(
      testM("Test scheduler with Job1")(
        managedTransactorBlocker.use { case (trans,_) =>
          for {
            jobs     <- getEtlJobs[MEJP](etlJob_name_package).map(jl => jl.filter(_.name == "Job1"))
            fiber    <- etlFlowScheduler(trans, testCronJobs, jobs).provideCustomLayer(testHttp4s(trans)).fork
            _        <- TestClock.adjust(ZDuration.fromScala(Duration(3,MINUTES)))
            _        <- fiber.interrupt
          } yield assertCompletes
        }
      ),
      testM("Test scheduler with no jobs")(
        managedTransactorBlocker.use { case (trans,_) =>
            etlFlowScheduler(trans, testCronJobs, List.empty).provideCustomLayer(testHttp4s(trans)).as(assertCompletes)
        }
      )
    ) @@ TestAspect.sequential
}

