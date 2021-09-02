package etlflow.utils

import etlflow.schema.WebServer
import zhttp.http.CORSConfig
import zio.test.Assertion.equalTo
import zio.test._

object CorsConfigTestSuite {

  val corsConfig1 = CorsConfig(Some(WebServer(None, None, None)))
  val corsConfig2 = CorsConfig(Some(WebServer(None, None, Some(Set("google.com")))))
  val corsConfig3 = CorsConfig(None)

  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("CorsConfig")(
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
