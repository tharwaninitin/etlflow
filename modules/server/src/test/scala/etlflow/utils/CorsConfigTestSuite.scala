package etlflow.utils

import etlflow.model.WebServer
import etlflow.webserver.middleware.CORSConfig
import zhttp.http.middleware.Cors.CorsConfig
import zio.test._

object CorsConfigTestSuite {

  val corsConfig1 = CORSConfig(Some(WebServer(None, None, None)))
  val corsConfig2 = CORSConfig(Some(WebServer(None, None, Some(Set("google.com")))))
  val corsConfig3 = CORSConfig(None)

  val spec: ZSpec[environment.TestEnvironment, Any] =
    suite("CorsConfig")(
      test("CorsConfig should return any origin as false when None is provided") {
        assertTrue(corsConfig1 == CorsConfig(anyOrigin = false, allowCredentials = false))
      },
      test("CorsConfig should return list of origins  when origins provided") {
        assertTrue(
          corsConfig2 == CorsConfig(anyOrigin = false, allowedOrigins = corsConfig2.allowedOrigins, allowCredentials = false)
        )
      },
      test("CorsConfig should return any origin as false when None is provided") {
        assertTrue(corsConfig3 == CorsConfig(anyOrigin = false, allowCredentials = false))
      }
    )
}
