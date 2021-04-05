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
import zio.{RIO, Runtime, Task, ZEnv, ZLayer}

object Http4sTestSuite extends DefaultRunnableSpec with TestEtlFlowService {

  zio.Runtime.default.unsafeRun(runDbMigration(credentials,clean = true))

  val env: ZLayer[Any, Throwable, Clock with Blocking with EtlFlowHas with Console] =
    Clock.live ++ Blocking.live ++ testHttp4s(transactor,cache) ++ Console.live

  val etlFlowInterpreter: Task[GraphQLInterpreter[Console with Clock with Blocking with EtlFlowHas, CalibanError]] =
    EtlFlowApi.api.interpreter

  val loginInterpreter1: Task[GraphQLInterpreter[Console with Clock with Blocking with EtlFlowHas, CalibanError]] =
    LoginApi.api.interpreter

  type EtlFlowTask[A] = RIO[ZEnv with EtlFlowHas, A]

  val runtime: Runtime[ZEnv] = Runtime.default

  val interpreter = runtime.unsafeRun(etlFlowInterpreter)
  val loginInterpreter = runtime.unsafeRun(loginInterpreter1)

  val routes: HttpRoutes[EtlFlowTask] = Router[EtlFlowTask](
    "/api/etlflow" -> AuthMiddleware(Http4sAdapter.makeHttpService(interpreter),authEnabled = true, cache),
     "/api/login" -> Http4sAdapter.makeHttpService(loginInterpreter)
  )

  val client: Client[EtlFlowTask] = Client.fromHttpApp[EtlFlowTask](routes.orNotFound)

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("Scheduler Http4s Spec")(
      testM("Test jobs end point with authorization header") {
        val loginBody = Json.obj("query" -> Json.fromString("""
                mutation
                {
                  login(user_name:"admin",password:"admin"){
                   token
                  }
                }"""))

        val loginRequest = Request[EtlFlowTask](method = POST, uri = uri"/api/login").withEntity(loginBody)
        val loginResponse = client.expect[String](loginRequest).provideCustomLayer(env)
        val apiBody = Json.obj("query" -> Json.fromString("{jobs {name}}"))

        val apiRequest = for {
          jsonOutput <- loginResponse
          authToken   = parse(jsonOutput).getOrElse(Json.Null).hcursor.downField("data").downField("login").downField("token").as[String].getOrElse("")
          apiRequest  = Request[EtlFlowTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("Authorization",authToken))).withEntity(apiBody)
        } yield apiRequest

        val apiResponse = client.expect[String](apiRequest).provideCustomLayer(env)
        assertM(apiResponse)(equalTo("""{"data":{"jobs":[{"name":"EtlJobDownload"},{"name":"Job1"}]}}"""))
      },
      testM("Test jobs end point without authorization header") {
        val apiBody = Json.obj("query" -> Json.fromString("{jobs {name}}"))
        val apiRequest = Request[EtlFlowTask](method = POST, uri = uri"/api/etlflow",headers = Headers.of(Header("Authorization",""))).withEntity(apiBody)
        val apiResponse =client.fetch(apiRequest) {
          case Status.Successful(r) => r.attemptAs[String].leftMap(_.message).value
          case r => r.as[String]
            .map(b => Left(s"${r.status.code}"))
        }.provideCustomLayer(env)
        assertM(apiResponse)(equalTo(Left("403")))
      }
    )
}
