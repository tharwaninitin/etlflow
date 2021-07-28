package etlflow.utils

import etlflow.ServerSuiteHelper
import zio.ZIO
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, _}

object SetTimeZoneTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {

  val setTime = SetTimeZone(config)

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("SetTimeZone")(
      testM("Set the provided time zone in config file ") {
        assertM(setTime.foldM(ex => ZIO.fail(ex.getMessage), _ => ZIO.succeed("Done")))(equalTo("Done"))
      }
    ).provideCustomLayerShared(testJsonLayer.orDie)
}
