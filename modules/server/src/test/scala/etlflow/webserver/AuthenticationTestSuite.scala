package etlflow.webserver

import etlflow.ServerSuiteHelper
import etlflow.api.Schema.UserArgs
import etlflow.db.RunDbMigration
import etlflow.executor.ExecutorTestSuite.credentials
import etlflow.utils.CacheHelper
import pdi.jwt.{Jwt, JwtAlgorithm}
import zhttp.http.Status.FORBIDDEN
import zhttp.http._
import zhttp.service.server.ServerChannelFactory
import zhttp.service.{ChannelFactory, EventLoopGroup}
import zio.test.Assertion.equalTo
import zio.test.{ZSpec, assertM, environment, _}


object AuthenticationTestSuite extends HttpRunnableSpec(8080) with ServerSuiteHelper  {

  zio.Runtime.default.unsafeRun(RunDbMigration(credentials,clean = true))

  val valid_login = auth.login(UserArgs("admin","admin"))
  val invalid_login = auth.login(UserArgs("admin","admin1"))

  val env = EventLoopGroup.auto() ++ ChannelFactory.auto ++ ServerChannelFactory.auto ++ (testAPILayer ++ testDBLayer ++ testJsonLayer).orDie

  val newRestApi = serve {
    auth.middleware(RestAPI.newRestApi)
  }

  val token: String = Jwt.encode("""{"user":"test"}""", auth.secret, JwtAlgorithm.HS256)
  val cachedToken: String = Jwt.encode("""{"user":"test1"}""", auth.secret, JwtAlgorithm.HS256)

  CacheHelper.putKey(authCache, cachedToken, cachedToken, Some(CacheHelper.default_ttl))

  override def spec: ZSpec[environment.TestEnvironment, Any] =
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
            testM("FORBIDDEN response when invalid header provided.") {
              val actual = statusPost(Root / "restapi" / "runjob" / "Job1", header = Some(List(Header("Authorization","12112112"))))
              assertM(actual)(equalTo(FORBIDDEN))
            },
            testM("FORBIDDEN response when no header provided.") {
              val actual = statusPost(Root / "restapi" / "runjob" / "Job1", None)
              assertM(actual)(equalTo(FORBIDDEN))
            },
            testM("Expired Token response when no header provided.") {
              val actual = statusPost(Root / "restapi" / "runjob" / "Job1", header = Some(List(Header("X-Auth-Token",token))))
              assertM(actual)(equalTo(FORBIDDEN))
            },
            testM("200 response when valid header provided.") {
              val actual = statusPost(Root / "restapi" / "runjob" / "Job1", header = Some(List(Header("X-Auth-Token",cachedToken))))
              assertM(actual)(equalTo(Status.OK))
            }
          )
        )
        .useNow
    )@@ TestAspect.sequential).provideCustomLayer(env)
}
