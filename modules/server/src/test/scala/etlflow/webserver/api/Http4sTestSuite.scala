package etlflow.webserver.api

import caliban.{CalibanError, GraphQLInterpreter, Http4sAdapter}
import etlflow.utils.EtlFlowHelper.EtlFlowHas
import io.circe.Json
import io.circe.parser._
import org.http4s.circe._
import org.http4s.client.Client
import org.http4s.dsl.io._
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.{HttpRoutes, _}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.interop.catz._
import zio.test.Assertion.equalTo
import zio.test._
import zio.{RIO, Runtime, Task, ZEnv, ZIO, ZLayer}

object Http4sTestSuite extends DefaultRunnableSpec with TestGqlImplementation {

  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))

  val env: ZLayer[Any, Throwable, Clock with Blocking with EtlFlowHas with Console] =
    Clock.live ++ Blocking.live ++ testHttp4s(transactor,cache) ++ Console.live

  val etlFlowInterpreter: Task[GraphQLInterpreter[Console with Clock with Blocking with EtlFlowHas, CalibanError]] =
    GqlAPI.api.interpreter

  val loginInterpreter1: Task[GraphQLInterpreter[Console with Clock with Blocking with EtlFlowHas, CalibanError]] =
    GqlLoginAPI.api.interpreter

  type EtlFlowTask[A] = RIO[ZEnv with EtlFlowHas, A]

  val runtime: Runtime[ZEnv] = Runtime.default

  val interpreter = runtime.unsafeRun(etlFlowInterpreter)
  val loginInterpreter = runtime.unsafeRun(loginInterpreter1)

  val routes: HttpRoutes[EtlFlowTask] = Router[EtlFlowTask](
    "/api/etlflow" -> Authentication.middleware(Http4sAdapter.makeHttpService(interpreter),authEnabled = true, cache),
     "/api/login" -> Http4sAdapter.makeHttpService(loginInterpreter)
  )

  private val client: Client[EtlFlowTask] = Client.fromHttpApp[EtlFlowTask](routes.orNotFound)
  private val apiBody = Json.obj("query" -> Json.fromString("{jobs {name}}"))
  private def apiResponse(apiRequest: Request[EtlFlowTask]): ZIO[ZEnv, Throwable, Either[String, String]] =
    client.fetch(apiRequest) {
      case Status.Successful(r) => r.attemptAs[String].leftMap(_.message).value
      case r => r.as[String].map(b => Left(s"${r.status.code}"))
    }.provideCustomLayer(env)

  override def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Http4s Test Suite")(
      testM("Test jobs end point with correct authorization header") {
        val loginBody = Json.obj("query" -> Json.fromString("""
                mutation
                {
                  login(user_name:"admin",password:"admin"){
                   token
                  }
                }"""))

        val loginRequest = Request[EtlFlowTask](method = POST, uri = uri"/api/login").withEntity(loginBody)
        val loginResponse = client.expect[String](loginRequest).provideCustomLayer(env)

        val apiRequest = for {
          jsonOutput <- loginResponse
          authToken   = parse(jsonOutput).getOrElse(Json.Null).hcursor.downField("data").downField("login").downField("token").as[String].getOrElse("")
          apiRequest  = Request[EtlFlowTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("Authorization",authToken))).withEntity(apiBody)
        } yield apiRequest

        val apiResponse = client.expect[String](apiRequest).provideCustomLayer(env)
        assertM(apiResponse)(equalTo("""{"data":{"jobs":[{"name":"EtlJobDownload"},{"name":"Job1"}]}}"""))
      },
      testM("Test jobs end point with blank authorization header") {
        val apiRequest = Request[EtlFlowTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("Authorization",""))).withEntity(apiBody)
        assertM(apiResponse(apiRequest))(equalTo(Left("403")))
      },
      testM("Test jobs end point with incorrect authorization header") {
        val apiRequest = Request[EtlFlowTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("Authorization","abcd"))).withEntity(apiBody)
        assertM(apiResponse(apiRequest))(equalTo(Left("403")))
      },
      testM("Test jobs end point without authorization header") {
        val apiRequest = Request[EtlFlowTask](method = POST, uri = uri"/api/etlflow").withEntity(apiBody)
        assertM(apiResponse(apiRequest))(equalTo(Left("403")))
      }
    ) @@ TestAspect.sequential
}
