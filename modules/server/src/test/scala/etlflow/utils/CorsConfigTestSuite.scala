package etlflow.utils

import etlflow.ServerSuiteHelper
import etlflow.db.JsonString
import etlflow.schema.WebServer
import zhttp.http.CORSConfig
import zio.test.Assertion.equalTo
import zio.test.{DefaultRunnableSpec, _}

object CorsConfigTestSuite extends DefaultRunnableSpec with ServerSuiteHelper {


  val corsConfig1 = CorsConfig(Some(WebServer(None, None, None, None)))
  val corsConfig2 = CorsConfig(Some(WebServer(None, None, None, Some(Set("google.com")))))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("CacheHelper")(
      test("CorsConfog should return any origin as false when None is provided") {
        assert(corsConfig1)(equalTo(CORSConfig(anyOrigin = false, allowCredentials = false)))
      },
      test("CorsConfog should return list of origins  when origins provided is provided") {
        assert(corsConfig2)(equalTo(CORSConfig(anyOrigin = false, allowedOrigins = corsConfig2.allowedOrigins, allowCredentials = false)))
      }
    ).provideCustomLayerShared(testJsonLayer.orDie)
}
