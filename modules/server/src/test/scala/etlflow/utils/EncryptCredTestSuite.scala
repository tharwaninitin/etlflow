package etlflow.utils

import etlflow.ServerSuiteHelper
import etlflow.db.JsonString
import etlflow.utils.RequestValidatorTestSuite.testJsonLayer
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, assert, _}

object EncryptCredTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {

  val aws_value = {
    """{
      |"access_key": "AKIA4FADZ4",
      |"secret_key": "ZiLo6CsbF6twGR"
      |}""".stripMargin
  }

  val expected_aws_encrypt = """{"access_key":"XRhxQfeHwehR/kvJGFbviw==","secret_key":"B67uKTvOC2B5GQEMAjnfPQ=="}"""


  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("CacheHelper")(
      testM("Provided aws values should be encrypted") {
        assertM(EncryptCred.apply("aws",JsonString(aws_value)))(equalTo("""{"access_key":"XRhxQfeHwehR/kvJGFbviw==","secret_key":"B67uKTvOC2B5GQEMAjnfPQ=="}"""))
      }
    ).provideCustomLayerShared(testJsonLayer.orDie)
}
