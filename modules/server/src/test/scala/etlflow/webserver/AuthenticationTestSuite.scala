package etlflow.webserver

import cache4s.default_ttl
import etlflow.ServerSuiteHelper
import etlflow.server.model.UserArgs
import etlflow.schema.Credential.JDBC
import pdi.jwt.{Jwt, JwtAlgorithm}
import zhttp.http._
import zio.test.Assertion.equalTo
import zio.test._

case class AuthenticationTestSuite(credential: JDBC, port: Int) extends HttpRunnableSpec(port) with ServerSuiteHelper {

  val newRestApi = serve {
    auth.middleware(RestAPI())
  }

  val token: String       = Jwt.encode("""{"user":"test"}""", auth.secret, JwtAlgorithm.HS256)
  val cachedToken: String = Jwt.encode("""{"user":"test1"}""", auth.secret, JwtAlgorithm.HS256)

  authCache.put(cachedToken, cachedToken, Some(default_ttl))

  val spec: ZSpec[environment.TestEnvironment with TestAuthEnv, Any] =
    (suiteM("Authentication")(
      newRestApi
        .as(
          List(
            testM("Authentication Test: Valid Login") {
              val valid_login = auth.login(UserArgs("admin", "admin"))
              assertM(valid_login.map(x => x.message))(equalTo("Valid User"))
            },
            testM("Authentication Test: Invalid Login") {
              val invalid_login = auth.login(UserArgs("admin", "admin1"))
              assertM(invalid_login.map(x => x.message))(equalTo("Invalid User/Password"))
            },
            testM("FORBIDDEN response when invalid Authorization header provided.") {
              val actual = statusPost(!! / "restapi" / "runjob" / "Job1", Headers("Authorization", "12112112"))
              assertM(actual)(equalTo(Status.FORBIDDEN))
            },
            testM("FORBIDDEN response when invalid X-Auth-Token header provided.") {
              val actual = statusPost(!! / "restapi" / "runjob" / "Job1", Headers("X-Auth-Token", "12112112"))
              assertM(actual)(equalTo(Status.FORBIDDEN))
            },
            testM("FORBIDDEN response when no header provided.") {
              val actual = statusPost(!! / "restapi" / "runjob" / "Job1")
              assertM(actual)(equalTo(Status.FORBIDDEN))
            },
            testM("Expired Token response when X-Auth-Token header provided.") {
              val actual = statusPost(!! / "restapi" / "runjob" / "Job1", Headers("X-Auth-Token", token))
              assertM(actual)(equalTo(Status.FORBIDDEN))
            },
            testM("Expired Token response when Authorization header provided.") {
              val actual = statusPost(!! / "restapi" / "runjob" / "Job1", Headers("Authorization", token))
              assertM(actual)(equalTo(Status.FORBIDDEN))
            },
            testM("200 response when valid  X-Auth-Token header provided.") {
              val actual = statusPost(!! / "restapi" / "runjob" / "Job1", Headers("X-Auth-Token", cachedToken))
              assertM(actual)(equalTo(Status.OK))
            },
            testM("200 response when valid Authorization  header provided.") {
              val actual = statusPost(!! / "restapi" / "runjob" / "Job1", Headers("Authorization", cachedToken))
              assertM(actual)(equalTo(Status.OK))
            }
          )
        )
        .useNow
    ) @@ TestAspect.sequential)
}
