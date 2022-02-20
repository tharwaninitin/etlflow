package etlflow.webserver

import cache4s.default_ttl
import etlflow.ServerSuiteHelper
import etlflow.json.JsonApi
import etlflow.server.model.{UserArgs, UserAuth}
import etlflow.model.Credential.JDBC
import pdi.jwt.{Jwt, JwtAlgorithm}
import zhttp.http._
import zio.test.Assertion.equalTo
import zio.test._

case class AuthenticatedRestTestSuite(credential: JDBC, port: Int) extends HttpRunnableSpec(port) with ServerSuiteHelper {

  val restApi = serve(RestAPI.login ++ auth.middleware(RestAPI.live))

  val token: String       = Jwt.encode("""{"user":"test"}""", auth.secret, JwtAlgorithm.HS256)
  val cachedToken: String = Jwt.encode("""{"user":"test1"}""", auth.secret, JwtAlgorithm.HS256)

  authCache.put(cachedToken, cachedToken, Some(default_ttl))

  val spec: ZSpec[environment.TestEnvironment with TestAuthEnv, Any] =
    suiteM("Authenticated Rest Api")(
      restApi
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
            testM("Login API: Valid Login Status") {
              val path = !! / "api" / "login"
              val body = """{"user_name":"admin", "password":"admin"}"""
              val req  = request(path, Headers.empty, Method.POST, body)
              assertM(req.map(_.status))(equalTo(Status.OK))
            },
            testM("Login API: Valid Login Body") {
              val path = !! / "api" / "login"
              val body = """{"user_name":"admin", "password":"admin"}"""
              val req = request(path, Headers.empty, Method.POST, body)
                .flatMap(_.bodyAsString)
                .flatMap(body => JsonApi.convertToObject[UserAuth](body)(zio.json.DeriveJsonDecoder.gen[UserAuth]))
                .map(_.message)
              assertM(req)(equalTo("Valid User"))
            },
            testM("Login API: InValid Login") {
              val path = !! / "api" / "login"
              val body = """{"user_name":"admin", "password":"admina"}"""
              val req = request(path, Headers.empty, Method.POST, body)
                .flatMap(_.bodyAsString)
                .flatMap(body => JsonApi.convertToObject[UserAuth](body)(zio.json.DeriveJsonDecoder.gen[UserAuth]))
                .map(_.message)
              assertM(req)(equalTo("Invalid User/Password"))
            },
            testM("FORBIDDEN response when invalid X-Auth-Token header provided.") {
              val actual = statusPost(!! / "api" / "etlflow" / "runjob" / "Job1", Headers("X-Auth-Token", "12112112"))
              assertM(actual)(equalTo(Status.FORBIDDEN))
            },
            testM("FORBIDDEN response when no header provided.") {
              val actual = statusPost(!! / "api" / "etlflow" / "runjob" / "Job1")
              assertM(actual)(equalTo(Status.FORBIDDEN))
            },
            testM("Expired Token response when X-Auth-Token header provided.") {
              val actual = statusPost(!! / "api" / "etlflow" / "runjob" / "Job1", Headers("X-Auth-Token", token))
              assertM(actual)(equalTo(Status.FORBIDDEN))
            },
            testM("200 response when valid  X-Auth-Token header provided.") {
              val actual = statusPost(!! / "api" / "etlflow" / "runjob" / "Job1", Headers("X-Auth-Token", cachedToken))
              assertM(actual)(equalTo(Status.OK))
            }
          )
        )
        .useNow
    ) @@ TestAspect.sequential
}
