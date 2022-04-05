package etlflow.utils

import etlflow.log.LogEnv
import etlflow.model.EtlFlowException.RetryException
import etlflow.task.GenericTask
import zio.clock.Clock
import zio.duration.{Duration => ZDuration}
import zio.test.Assertion.equalTo
import zio.test.environment.TestClock
import zio.test.{assertM, environment, suite, testM, ZSpec}
import zio.{RIO, ZIO}
import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object RetryStepTestSuite extends ApplicationLogger {
  val spec: ZSpec[environment.TestEnvironment with LogEnv, Any] =
    suite("Retry Step")(
      testM("Execute GenericETLStep with retry") {
        def processDataFail(): Unit = {
          logger.info("Hello World")
          throw RetryException("Failed in processing data")
        }

        val step: RIO[Clock with LogEnv, Unit] = GenericTask(
          name = "ProcessData",
          function = processDataFail()
        ).executeZio.retry(RetrySchedule(2, 5.second))

        val program = for {
          s <- step.fork
          _ <- TestClock.adjust(ZDuration.fromScala(15.second))
          _ <- s.join
        } yield ()

        assertM(program.foldM(ex => ZIO.succeed(ex.getMessage), _ => ZIO.succeed("ok")))(
          equalTo("Failed in processing data")
        )
      }
    )
}
