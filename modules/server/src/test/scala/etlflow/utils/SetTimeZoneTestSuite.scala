package etlflow.utils

import etlflow.model.Config
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test._

case class SetTimeZoneTestSuite(config: Config) {
  val setTime = SetTimeZone(config)
  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("SetTimeZone")(
      testM("Set the provided time zone in config file ") {
        assertM(setTime.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      }
    )
}
