package etlflow.webserver

import etlflow.api.Schema.UserArgs
import etlflow.cache.{CacheApi, default_ttl}
import etlflow.db.RunDbMigration
import etlflow.schema.Credential.JDBC
import pdi.jwt.{Jwt, JwtAlgorithm}
import zhttp.http.Status.FORBIDDEN
import zhttp.http._
import zhttp.service.server.ServerChannelFactory
import zhttp.service.{ChannelFactory, EventLoopGroup}
import zio.Runtime.default.unsafeRun
import zio.test.Assertion.equalTo
import zio.test._


case class AuthenticationTestSuite(credential: JDBC) extends HttpRunnableSpec(8081) {

  val valid_login = auth.login(UserArgs("admin","admin"))
  val invalid_login = auth.login(UserArgs("admin","admin1"))

  val env = EventLoopGroup.auto() ++ ChannelFactory.auto ++ ServerChannelFactory.auto ++ (fullLayer).orDie

  val newRestApi = serve {
    auth.middleware(RestAPI.newRestApi)
  }

  val token: String = Jwt.encode("""{"user":"test"}""", auth.secret, JwtAlgorithm.HS256)
  val cachedToken: String = Jwt.encode("""{"user":"test1"}""", auth.secret, JwtAlgorithm.HS256)

  unsafeRun(CacheApi.put(authCache, cachedToken, cachedToken, Some(default_ttl)).provideCustomLayer(testCacheLayer))

  val spec: ZSpec[environment.TestEnvironment, Any] =
    (suiteM("Authentication Test Suite")(
      newRestApi
        .as(
          List(
            testM("Authentication Test: Valid Login")(
               assertM(valid_login.map(x=> x.message))(equalTo("Valid User"))
             ),
            testM("Authentication Test: Invalid Login")(
               assertM(invalid_login.map(x=> x.message))(equalTo("Invalid User/Password"))
             ),
            testM("FORBIDDEN response when invalid Authorization header provided.") {
              val actual = statusPost(Root / "restapi" / "runjob" / "Job1", header = Some(List(Header("Authorization","12112112"))))
              assertM(actual)(equalTo(FORBIDDEN))
            },
            testM("FORBIDDEN response when invalid X-Auth-Token header provided.") {
              val actual = statusPost(Root / "restapi" / "runjob" / "Job1", header = Some(List(Header("X-Auth-Token","12112112"))))
              assertM(actual)(equalTo(FORBIDDEN))
            },
            testM("FORBIDDEN response when no header provided.") {
              val actual = statusPost(Root / "restapi" / "runjob" / "Job1", None)
              assertM(actual)(equalTo(FORBIDDEN))
            },
            testM("Expired Token response when X-Auth-Token header provided.") {
              val actual = statusPost(Root / "restapi" / "runjob" / "Job1", header = Some(List(Header("X-Auth-Token",token))))
              assertM(actual)(equalTo(FORBIDDEN))
            },
            testM("Expired Token response when Authorization header provided.") {
              val actual = statusPost(Root / "restapi" / "runjob" / "Job1", header = Some(List(Header("Authorization",token))))
              assertM(actual)(equalTo(FORBIDDEN))
            },
            testM("200 response when valid  X-Auth-Token header provided.") {
              val actual = statusPost(Root / "restapi" / "runjob" / "Job1", header = Some(List(Header("X-Auth-Token",cachedToken))))
              assertM(actual)(equalTo(Status.OK))
            },
            testM("200 response when valid Authorization  header provided.") {
              val actual = statusPost(Root / "restapi" / "runjob" / "Job1", header = Some(List(Header("Authorization",cachedToken))))
              assertM(actual)(equalTo(Status.OK))
            }
          )
        )
        .useNow
    )@@ TestAspect.sequential).provideCustomLayer(env)
}
