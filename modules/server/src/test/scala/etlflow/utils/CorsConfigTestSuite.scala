package etlflow.utils

import etlflow.ServerSuiteHelper
import etlflow.schema.WebServer
import zhttp.http.CORSConfig
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, _}

object CorsConfigTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {


  val corsConfig1 = CorsConfig(Some(WebServer(None, None, None, None)))
  val corsConfig2 = CorsConfig(Some(WebServer(None, None, None, Some(Set("google.com")))))
  val corsConfig3 = CorsConfig(None)

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("CorsConfig Test Suite")(
      test("CorsConfig should return any origin as false when None is provided") {
        assert(corsConfig1)(equalTo(CORSConfig(anyOrigin = false, allowCredentials = false)))
      },
      test("CorsConfig should return list of origins  when origins provided") {
        assert(corsConfig2)(equalTo(CORSConfig(anyOrigin = false, allowedOrigins = corsConfig2.allowedOrigins, allowCredentials = false)))
      },
      test("CorsConfig should return any origin as false when None is provided") {
        assert(corsConfig3)(equalTo(CORSConfig(anyOrigin = false, allowCredentials = false)))
      }
    )
}
